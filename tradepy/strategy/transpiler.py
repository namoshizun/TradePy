import ast
import inspect
import textwrap
from dataclasses import dataclass
from typing import Any, Callable

import polars as pl


@dataclass(frozen=True)
class _PathState:
    guards: list[pl.Expr]
    locals: dict[str, pl.Expr]


@dataclass(frozen=True)
class _ValueReturn:
    guard: pl.Expr
    value: pl.Expr


class PolarsExprTranspiler:
    """
    Inspects the AST of a bool-returning method at runtime and transpiles
    it into a native Polars expression. Users write pure Python; this
    class handles the conversion to Rust-speed execution.

    Supported constructs:
      - Comparisons:       <, <=, >, >=, ==, !=
      - Boolean ops:       and, or, not
      - Arithmetic:        +, -, *, /, **
      - Truthy checks:     if col_name:
      - self.attr:         self.conf.x  →  resolved to a Python literal
      - self.method(col):  inlined by recursively parsing the method's AST
      - Built-in round():  round(expr, n)  →  expr.round(n)
    """

    def __init__(self, instance: Any):
        self._instance = instance
        self._param_names: set[str] = set()

    def transpile(self, method_name: str, alias: str | None = None) -> pl.Expr:
        method = getattr(self._instance, method_name)
        func_def = self._parse_func(method)

        self._param_names = self._collect_param_names(func_def)

        branches, _ = self._eval_bool_stmts(
            func_def.body,
            [_PathState(guards=[], locals={})],
        )

        if not branches:
            expr = pl.lit(False)
        else:
            expr = branches[0]
            for b in branches[1:]:
                expr = expr | b

        return expr.alias(alias or method_name)

    # ------------------------------------------------------------------ #
    #  AST traversal                                                       #
    # ------------------------------------------------------------------ #

    def _eval_bool_stmts(
        self, stmts: list[ast.stmt], states: list[_PathState]
    ) -> tuple[list[pl.Expr], list[_PathState]]:
        branches: list[pl.Expr] = []
        live_states = states

        for stmt in stmts:
            next_states: list[_PathState] = []
            for state in live_states:
                stmt_branches, stmt_states = self._eval_bool_stmt(stmt, state)
                branches.extend(stmt_branches)
                next_states.extend(stmt_states)
            live_states = next_states
            if not live_states:
                break

        return branches, live_states

    def _eval_bool_stmt(
        self, stmt: ast.stmt, state: _PathState
    ) -> tuple[list[pl.Expr], list[_PathState]]:
        if isinstance(stmt, ast.Return):
            return self._eval_bool_return(stmt, state), []

        if isinstance(stmt, ast.If):
            test = self._to_bool_expr(stmt.test, state.locals)
            body_state = _PathState(
                guards=state.guards + [test],
                locals=dict(state.locals),
            )
            else_state = _PathState(
                guards=state.guards + [~test],
                locals=dict(state.locals),
            )

            body_branches, body_live = self._eval_bool_stmts(
                stmt.body, [body_state]
            )
            if stmt.orelse:
                else_branches, else_live = self._eval_bool_stmts(
                    stmt.orelse, [else_state]
                )
            else:
                else_branches, else_live = [], [else_state]

            return body_branches + else_branches, body_live + else_live

        if isinstance(stmt, (ast.Assign, ast.AnnAssign)):
            return [], [self._assign_local(stmt, state)]

        if isinstance(stmt, ast.Pass):
            return [], [state]

        if self._is_docstring_expr(stmt):
            return [], [state]

        raise NotImplementedError(
            f"Unsupported statement: {ast.unparse(stmt)!r}"
        )

    def _eval_bool_return(
        self, stmt: ast.Return, state: _PathState
    ) -> list[pl.Expr]:
        value = stmt.value
        if value is None:
            return []
        if isinstance(value, ast.Constant):
            if value.value is True:
                return [self._guard_expr(state.guards)]
            if value.value in (False, None):
                return []
            raise NotImplementedError(
                f"Unsupported bool return: {ast.unparse(value)!r}"
            )

        expr = self._to_bool_expr(value, state.locals)
        if state.guards:
            expr = self._and_all(state.guards) & expr
        return [expr]

    def _eval_value_stmts(
        self, stmts: list[ast.stmt], states: list[_PathState]
    ) -> tuple[list[_ValueReturn], list[_PathState]]:
        returns: list[_ValueReturn] = []
        live_states = states

        for stmt in stmts:
            next_states: list[_PathState] = []
            for state in live_states:
                stmt_returns, stmt_states = self._eval_value_stmt(stmt, state)
                returns.extend(stmt_returns)
                next_states.extend(stmt_states)
            live_states = next_states
            if not live_states:
                break

        return returns, live_states

    def _eval_value_stmt(
        self, stmt: ast.stmt, state: _PathState
    ) -> tuple[list[_ValueReturn], list[_PathState]]:
        if isinstance(stmt, ast.Return):
            if stmt.value is None:
                raise ValueError("Helper methods must return a value")
            return [
                _ValueReturn(
                    guard=self._guard_expr(state.guards),
                    value=self._to_value_expr(stmt.value, state.locals),
                )
            ], []

        if isinstance(stmt, ast.If):
            test = self._to_bool_expr(stmt.test, state.locals)
            body_state = _PathState(
                guards=state.guards + [test],
                locals=dict(state.locals),
            )
            else_state = _PathState(
                guards=state.guards + [~test],
                locals=dict(state.locals),
            )

            body_returns, body_live = self._eval_value_stmts(
                stmt.body, [body_state]
            )
            if stmt.orelse:
                else_returns, else_live = self._eval_value_stmts(
                    stmt.orelse, [else_state]
                )
            else:
                else_returns, else_live = [], [else_state]

            return body_returns + else_returns, body_live + else_live

        if isinstance(stmt, (ast.Assign, ast.AnnAssign)):
            return [], [self._assign_local(stmt, state)]

        if isinstance(stmt, ast.Pass):
            return [], [state]

        if self._is_docstring_expr(stmt):
            return [], [state]

        raise NotImplementedError(
            f"Unsupported statement: {ast.unparse(stmt)!r}"
        )

    # ------------------------------------------------------------------ #
    #  Boolean expression conversion                                       #
    # ------------------------------------------------------------------ #

    def _to_bool_expr(
        self, node: ast.expr, local_map: dict[str, pl.Expr] | None = None
    ) -> pl.Expr:
        local_map = local_map or {}
        if isinstance(node, ast.Compare):
            return self._compare(node, local_map)
        if isinstance(node, ast.BoolOp):
            return self._boolop(node, local_map)
        if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
            return ~self._to_bool_expr(node.operand, local_map)
        if isinstance(node, ast.Name) and node.id in local_map:
            return local_map[node.id].cast(pl.Boolean)
        if isinstance(node, ast.Name) and node.id in self._param_names:
            # bare `if col_name:` truthy check
            return pl.col(node.id).cast(pl.Boolean)
        if isinstance(node, ast.Call):
            return self._call(node, local_map).cast(pl.Boolean)
        if isinstance(node, ast.Constant):
            return pl.lit(bool(node.value))
        raise NotImplementedError(
            f"Unsupported condition: {ast.unparse(node)!r}\n"
            "Consider extracting this into a helper method."
        )

    def _compare(
        self, node: ast.Compare, local_map: dict[str, pl.Expr] | None = None
    ) -> pl.Expr:
        left = self._to_value_expr(node.left, local_map)
        result: pl.Expr | None = None
        _ops = {
            ast.Lt: lambda left_expr, right_expr: left_expr < right_expr,
            ast.LtE: lambda left_expr, right_expr: left_expr <= right_expr,
            ast.Gt: lambda left_expr, right_expr: left_expr > right_expr,
            ast.GtE: lambda left_expr, right_expr: left_expr >= right_expr,
            ast.Eq: lambda left_expr, right_expr: left_expr == right_expr,
            ast.NotEq: lambda left_expr, right_expr: left_expr != right_expr,
        }
        for op, comp in zip(node.ops, node.comparators):
            if type(op) not in _ops:
                raise NotImplementedError(
                    f"Unsupported comparison operator: {type(op).__name__}"
                )
            right = self._to_value_expr(comp, local_map)
            cmp = _ops[type(op)](left, right)
            result = cmp if result is None else result & cmp
            left = right
        if result is None:
            raise NotImplementedError(
                f"Empty comparison: {ast.unparse(node)!r}"
            )
        return result

    def _boolop(
        self, node: ast.BoolOp, local_map: dict[str, pl.Expr] | None = None
    ) -> pl.Expr:
        exprs = [self._to_bool_expr(v, local_map) for v in node.values]
        result = exprs[0]
        for e in exprs[1:]:
            result = (
                (result & e) if isinstance(node.op, ast.And) else (result | e)
            )
        return result

    # ------------------------------------------------------------------ #
    #  Value expression conversion (operands, arithmetic, calls)          #
    # ------------------------------------------------------------------ #

    def _to_value_expr(
        self, node: ast.expr, local_map: dict[str, pl.Expr] | None = None
    ) -> pl.Expr:
        local_map = local_map or {}

        if isinstance(node, ast.Name):
            if node.id in local_map:
                return local_map[node.id]
            if node.id in self._param_names:
                return pl.col(node.id)
            raise NameError(
                f"Unknown name '{node.id}' — not a function parameter."
            )

        if isinstance(node, ast.Constant):
            return pl.lit(node.value)

        if isinstance(node, ast.Attribute):
            # self.conf.threshold → evaluate on live instance → Python literal
            return pl.lit(self._resolve_attr(node))

        if isinstance(node, ast.BinOp):
            return self._binop(node, local_map)

        if isinstance(node, ast.UnaryOp):
            return self._unaryop(node, local_map)

        if isinstance(node, ast.Call):
            return self._call(node, local_map)

        if isinstance(node, (ast.Compare, ast.BoolOp)):
            return self._to_bool_expr(node, local_map)

        raise NotImplementedError(
            f"Unsupported value expression: {ast.unparse(node)!r}"
        )

    def _binop(self, node: ast.BinOp, local_map: dict[str, pl.Expr]) -> pl.Expr:
        left = self._to_value_expr(node.left, local_map)
        right = self._to_value_expr(node.right, local_map)
        _ops = {
            ast.Add: lambda a, b: a + b,
            ast.Sub: lambda a, b: a - b,
            ast.Mult: lambda a, b: a * b,
            ast.Div: lambda a, b: a / b,
            ast.Pow: lambda a, b: a**b,
        }
        if type(node.op) not in _ops:
            raise NotImplementedError(
                f"Unsupported operator: {type(node.op).__name__}"
            )
        return _ops[type(node.op)](left, right)

    def _unaryop(
        self, node: ast.UnaryOp, local_map: dict[str, pl.Expr]
    ) -> pl.Expr:
        value = self._to_value_expr(node.operand, local_map)
        if isinstance(node.op, ast.USub):
            return -value
        if isinstance(node.op, ast.UAdd):
            return value
        if isinstance(node.op, ast.Not):
            return ~value.cast(pl.Boolean)
        raise NotImplementedError(
            f"Unsupported unary operator: {type(node.op).__name__}"
        )

    def _call(self, node: ast.Call, local_map: dict[str, pl.Expr]) -> pl.Expr:
        # round(expr, ndigits)
        if isinstance(node.func, ast.Name) and node.func.id == "round":
            inner = self._to_value_expr(node.args[0], local_map)
            if len(node.args) > 1:
                ndigits_node = node.args[1]
                if not isinstance(ndigits_node, ast.Constant):
                    raise NotImplementedError(
                        "round() ndigits must be a Python literal"
                    )
                ndigits = ndigits_node.value
                if not isinstance(ndigits, int):
                    raise TypeError("round() ndigits must be an int")
            else:
                ndigits = 0
            return inner.round(ndigits)

        # self.method(args...)  →  inline the method
        if (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "self"
        ):
            return self._inline_method(node.func.attr, node.args, local_map)

        raise NotImplementedError(f"Unsupported call: {ast.unparse(node)!r}")

    # ------------------------------------------------------------------ #
    #  Helper method inlining (handles name-mangled private methods too)  #
    # ------------------------------------------------------------------ #

    def _inline_method(
        self,
        name: str,
        arg_nodes: list[ast.expr],
        caller_locals: dict[str, pl.Expr],
    ) -> pl.Expr:
        """
        Parse a self.method() call, map its parameters to the caller's
        Polars expressions, and return the formula as a Polars Expr.
        """
        method = self._resolve_method(name)
        func_def = self._parse_func(method)

        param_args = self._collect_param_args(func_def)
        param_names = {arg.arg for arg in param_args}
        if len(arg_nodes) != len(param_args):
            raise TypeError(
                f"Method '{name}' expects {len(param_args)} args, got {len(arg_nodes)}"
            )
        local_map = {
            param.arg: self._to_value_expr(arg_node, caller_locals)
            for param, arg_node in zip(param_args, arg_nodes)
        }

        outer_param_names = self._param_names
        try:
            self._param_names = param_names
            returns, live_states = self._eval_value_stmts(
                func_def.body,
                [_PathState(guards=[], locals=local_map)],
            )
        finally:
            self._param_names = outer_param_names

        if live_states:
            raise ValueError(
                f"Method '{name}' has a path that does not return a value"
            )
        if not returns:
            raise ValueError(f"No return statement found in method '{name}'")
        if len(returns) == 1:
            return returns[0].value

        expr = pl.when(returns[0].guard).then(returns[0].value)
        for branch in returns[1:-1]:
            expr = expr.when(branch.guard).then(branch.value)
        return expr.otherwise(returns[-1].value)

    def _resolve_attr(self, node: ast.Attribute) -> Any:
        parts: list[str] = []
        current: ast.expr = node
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if not isinstance(current, ast.Name) or current.id != "self":
            raise NotImplementedError(
                f"Only self.<attr> chains are supported: {ast.unparse(node)!r}"
            )
        obj: Any = self._instance
        for attr in reversed(parts):
            obj = getattr(obj, attr)
        return obj

    def _resolve_method(self, name: str):
        """Handles Python name mangling: __method → _ClassName__method."""
        if hasattr(self._instance, name):
            return getattr(self._instance, name)
        # '__foo' in class Strategy → '_Strategy__foo'; same for '_Strategy' classes
        cls_name = type(self._instance).__name__.lstrip("_")
        mangled = f"_{cls_name}__{name.removeprefix('__')}"
        if hasattr(self._instance, mangled):
            return getattr(self._instance, mangled)
        raise AttributeError(
            f"Cannot resolve '{name}' on {type(self._instance).__name__}"
        )

    # ------------------------------------------------------------------ #
    #  Utilities                                                           #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _parse_func(method: Callable) -> ast.FunctionDef:
        source = textwrap.dedent(inspect.getsource(method))
        node = ast.parse(source).body[0]
        if not isinstance(node, ast.FunctionDef):
            raise TypeError("Expected a function definition")
        return node

    @staticmethod
    def _collect_param_names(func_def: ast.FunctionDef) -> set[str]:
        return {
            arg.arg
            for arg in PolarsExprTranspiler._collect_param_args(func_def)
        }

    @staticmethod
    def _collect_param_args(func_def: ast.FunctionDef) -> tuple[ast.arg, ...]:
        args = (
            *func_def.args.posonlyargs,
            *func_def.args.args,
            *func_def.args.kwonlyargs,
        )
        return tuple(arg for arg in args if arg.arg != "self")

    def _assign_local(
        self, stmt: ast.Assign | ast.AnnAssign, state: _PathState
    ) -> _PathState:
        if isinstance(stmt, ast.Assign):
            if len(stmt.targets) != 1 or not isinstance(
                stmt.targets[0], ast.Name
            ):
                raise NotImplementedError(
                    f"Only single-name assignments are supported: {ast.unparse(stmt)!r}"
                )
            name = stmt.targets[0].id
            value_node = stmt.value
        else:
            if not isinstance(stmt.target, ast.Name) or stmt.value is None:
                raise NotImplementedError(
                    f"Only initialized name annotations are supported: {ast.unparse(stmt)!r}"
                )
            name = stmt.target.id
            value_node = stmt.value

        local_map = dict(state.locals)
        local_map[name] = self._to_value_expr(value_node, state.locals)
        return _PathState(guards=state.guards, locals=local_map)

    @staticmethod
    def _is_docstring_expr(stmt: ast.stmt) -> bool:
        return (
            isinstance(stmt, ast.Expr)
            and isinstance(stmt.value, ast.Constant)
            and isinstance(stmt.value.value, str)
        )

    def _guard_expr(self, guards: list[pl.Expr]) -> pl.Expr:
        return self._and_all(guards) if guards else pl.lit(True)

    @staticmethod
    def _and_all(exprs: list[pl.Expr]) -> pl.Expr:
        result = exprs[0]
        for e in exprs[1:]:
            result = result & e
        return result
