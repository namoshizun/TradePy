import ast
import inspect
import operator
import textwrap
from dataclasses import dataclass, field
from functools import reduce
from typing import Any, Callable

import polars as pl

_UNKNOWN = object()

_BinaryOp = Callable[[pl.Expr, pl.Expr], pl.Expr]

_BIN_OPS: dict[type[ast.operator], _BinaryOp] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Pow: operator.pow,
}

_CMP_OPS: dict[type[ast.cmpop], _BinaryOp] = {
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
}

_BOOL_OPS: dict[type[ast.boolop], _BinaryOp] = {
    ast.And: operator.and_,
    ast.Or: operator.or_,
}


@dataclass(frozen=True)
class _Scope:
    """Name resolution context: locals shadow dataframe columns."""

    columns: frozenset[str]
    string_columns: frozenset[str] = frozenset()
    locals: dict[str, pl.Expr] = field(default_factory=dict)

    def resolve(self, name: str) -> pl.Expr:
        if name in self.locals:
            return self.locals[name]
        if name in self.columns:
            col = pl.col(name)
            return (
                col.cast(pl.String) if name in self.string_columns else col
            )
        raise NameError(
            f"Unknown name '{name}' — not a function parameter or local."
        )

    def assign(self, name: str, value: pl.Expr) -> "_Scope":
        return _Scope(
            columns=self.columns,
            string_columns=self.string_columns,
            locals={**self.locals, name: value},
        )


@dataclass(frozen=True)
class _Path:
    """One control-flow path: the conditions to reach it, plus its scope."""

    scope: _Scope
    guards: tuple[pl.Expr, ...] = ()

    @property
    def guard(self) -> pl.Expr:
        return (
            reduce(operator.and_, self.guards) if self.guards else pl.lit(True)
        )

    def narrowed(self, condition: pl.Expr) -> "_Path":
        return _Path(scope=self.scope, guards=(*self.guards, condition))

    def with_scope(self, scope: _Scope) -> "_Path":
        return _Path(scope=scope, guards=self.guards)


@dataclass(frozen=True)
class _Branch:
    """A `return` reached under `guard`, yielding `value`."""

    guard: pl.Expr
    value: pl.Expr


class PolarsExprTranspiler:
    """
    Inspects the AST of a price-returning method at runtime and transpiles
    it into a native Polars expression. Users write pure Python; this class
    handles the conversion to Rust-speed execution.

    Supported constructs:
      - Comparisons:       <, <=, >, >=, ==, !=, in, not in (incl. chained)
      - Boolean ops:       and, or, not
      - Arithmetic:        +, -, *, /, **
      - Truthy checks:     if col_name:
      - Local variables:   x = ...; later conditions/returns may use x
      - self.attr:         self.conf.x  →  resolved to a Python literal
      - self.method(col):  inlined by recursively transpiling the method
      - Built-in round():  round(expr, n)  →  expr.round(n)

    The method body is flattened into a set of (guard, value) branches —
    one per reachable `return` — and combined into a single
    when/then/otherwise chain.
    """

    def __init__(self, instance: Any):
        self._instance = instance

    def transpile(self, method_name: str, alias: str | None = None) -> pl.Expr:
        func_def = self._parse_func(getattr(self._instance, method_name))
        scope = self._param_scope(func_def)
        branches, _ = self._walk_block(func_def.body, [_Path(scope=scope)])

        if not branches:
            expr = pl.lit(None)
        else:
            chain = pl.when(branches[0].guard).then(branches[0].value)
            for branch in branches[1:]:
                chain = chain.when(branch.guard).then(branch.value)
            expr = chain.otherwise(None)

        return expr.alias(alias or method_name)

    # ------------------------------------------------------------------ #
    #  Statement walking                                                   #
    # ------------------------------------------------------------------ #

    def _walk_block(
        self, stmts: list[ast.stmt], paths: list[_Path]
    ) -> tuple[list[_Branch], list[_Path]]:
        """
        Walk statements along every live path. Returns the branches produced
        by `return` statements, and the paths that fell through the block.
        """
        branches: list[_Branch] = []
        for stmt in stmts:
            if not paths:
                break
            next_paths: list[_Path] = []
            for path in paths:
                stmt_branches, live = self._walk_stmt(stmt, path)
                branches.extend(stmt_branches)
                next_paths.extend(live)
            paths = next_paths
        return branches, paths

    def _walk_stmt(
        self, stmt: ast.stmt, path: _Path
    ) -> tuple[list[_Branch], list[_Path]]:
        match stmt:
            case ast.Return(value=value):
                expr = (
                    pl.lit(None)
                    if value is None
                    else self._expr(value, path.scope)
                )
                return [_Branch(guard=path.guard, value=expr)], []

            case ast.If(test=test, body=body, orelse=orelse):
                condition = self._condition(test, path.scope)
                then_branches, then_live = self._walk_block(
                    body, [path.narrowed(condition)]
                )
                else_branches, else_live = self._walk_block(
                    orelse, [path.narrowed(~condition)]
                )
                return then_branches + else_branches, then_live + else_live

            case ast.Assign() | ast.AnnAssign():
                name, value_node = self._assign_target(stmt)
                value = self._expr(value_node, path.scope)
                return [], [path.with_scope(path.scope.assign(name, value))]

            case ast.Pass():
                return [], [path]

            case ast.Expr(value=ast.Constant(value=str())):  # docstring
                return [], [path]

            case _:
                raise NotImplementedError(
                    f"Unsupported statement: {ast.unparse(stmt)!r}"
                )

    @staticmethod
    def _assign_target(
        stmt: ast.Assign | ast.AnnAssign,
    ) -> tuple[str, ast.expr]:
        match stmt:
            case ast.Assign(targets=[ast.Name(id=name)], value=value):
                return name, value
            case ast.AnnAssign(
                target=ast.Name(id=name), value=value
            ) if value is not None:
                return name, value
        raise NotImplementedError(
            f"Only single-name assignments are supported: {ast.unparse(stmt)!r}"
        )

    # ------------------------------------------------------------------ #
    #  Expression conversion                                              #
    # ------------------------------------------------------------------ #

    def _expr(self, node: ast.expr, scope: _Scope) -> pl.Expr:
        match node:
            case ast.Name(id=name):
                return scope.resolve(name)

            case ast.Constant(value=value):
                return pl.lit(value)

            case ast.Attribute():
                # self.conf.threshold → evaluated on the live instance
                return pl.lit(self._self_attr(node))

            case ast.BinOp(left=left, op=op, right=right):
                apply = self._op(_BIN_OPS, op)
                return apply(self._expr(left, scope), self._expr(right, scope))

            case ast.UnaryOp(op=ast.USub(), operand=operand):
                return -self._expr(operand, scope)

            case ast.UnaryOp(op=ast.UAdd(), operand=operand):
                return self._expr(operand, scope)

            case ast.UnaryOp(op=ast.Not(), operand=operand):
                return ~self._condition(operand, scope)

            case ast.Call():
                return self._call(node, scope)

            case ast.Compare() | ast.BoolOp():
                return self._condition(node, scope)

            case _:
                raise NotImplementedError(
                    f"Unsupported value expression: {ast.unparse(node)!r}"
                )

    def _condition(self, node: ast.expr, scope: _Scope) -> pl.Expr:
        match node:
            case ast.Compare():
                return self._compare(node, scope)

            case ast.BoolOp(op=op, values=values):
                apply = self._op(_BOOL_OPS, op)
                return reduce(
                    apply, (self._condition(v, scope) for v in values)
                )

            case ast.UnaryOp(op=ast.Not(), operand=operand):
                return ~self._condition(operand, scope)

            case ast.Constant(value=value):
                return pl.lit(bool(value))

            case ast.Name() | ast.Call():  # truthy check, e.g. `if flag:`
                return self._expr(node, scope).cast(pl.Boolean)

            case _:
                raise NotImplementedError(
                    f"Unsupported condition: {ast.unparse(node)!r}\n"
                    "Consider extracting this into a helper method."
                )

    def _compare(self, node: ast.Compare, scope: _Scope) -> pl.Expr:
        # `a < b < c` desugars to pairwise comparisons ANDed together
        operands = [node.left, *node.comparators]
        comparisons: list[pl.Expr] = []
        for op, left, right in zip(node.ops, operands, operands[1:]):
            if isinstance(op, (ast.In, ast.NotIn)):
                cmp = self._membership(left, right, scope)
                if isinstance(op, ast.NotIn):
                    cmp = ~cmp
            else:
                apply = self._op(_CMP_OPS, op)
                cmp = apply(self._expr(left, scope), self._expr(right, scope))
            comparisons.append(cmp)
        return reduce(operator.and_, comparisons)

    def _membership(
        self, left_node: ast.expr, container_node: ast.expr, scope: _Scope
    ) -> pl.Expr:
        container = self._static_value(container_node)

        if isinstance(container, str):
            # "needle" in "literal string" → substring check
            return pl.lit(container).str.contains(
                self._expr(left_node, scope), literal=True
            )
        if isinstance(container, (list, tuple, set, frozenset, dict)):
            return self._expr(left_node, scope).is_in(list(container))
        if container is not _UNKNOWN:
            raise NotImplementedError(
                f"Unsupported membership container: {ast.unparse(container_node)!r}"
            )

        # Dynamic container, e.g. "ST" in name → substring check on a column
        return self._expr(container_node, scope).str.contains(
            self._expr(left_node, scope), literal=True
        )

    def _call(self, node: ast.Call, scope: _Scope) -> pl.Expr:
        match node.func:
            case ast.Name(id="round"):
                return self._round(node, scope)
            case ast.Attribute(value=ast.Name(id="self"), attr=method_name):
                return self._inline_method(method_name, node.args, scope)
        raise NotImplementedError(f"Unsupported call: {ast.unparse(node)!r}")

    def _round(self, node: ast.Call, scope: _Scope) -> pl.Expr:
        value = self._expr(node.args[0], scope)
        ndigits = 0
        if len(node.args) > 1:
            match node.args[1]:
                case ast.Constant(value=int() as ndigits):
                    pass
                case _:
                    raise NotImplementedError(
                        "round() ndigits must be an int literal"
                    )
        return value.round(ndigits)

    # ------------------------------------------------------------------ #
    #  Helper method inlining                                             #
    # ------------------------------------------------------------------ #

    def _inline_method(
        self,
        name: str,
        arg_nodes: list[ast.expr],
        caller_scope: _Scope,
    ) -> pl.Expr:
        """
        Transpile a self.method() call by binding its parameters to the
        caller's argument expressions and flattening its body.
        """
        func_def = self._parse_func(self._resolve_method(name))
        params = self._params(func_def)
        if len(arg_nodes) != len(params):
            raise TypeError(
                f"Method '{name}' expects {len(params)} args, got {len(arg_nodes)}"
            )

        # The helper sees only its own parameters — caller names don't leak
        scope = _Scope(
            columns=frozenset(),
            locals={
                param.arg: self._expr(arg_node, caller_scope)
                for param, arg_node in zip(params, arg_nodes)
            },
        )
        branches, live_paths = self._walk_block(
            func_def.body, [_Path(scope=scope)]
        )

        if live_paths:
            raise ValueError(
                f"Method '{name}' has a path that does not return a value"
            )
        if not branches:
            raise ValueError(f"No return statement found in method '{name}'")
        if len(branches) == 1:
            return branches[0].value

        chain = pl.when(branches[0].guard).then(branches[0].value)
        for branch in branches[1:-1]:
            chain = chain.when(branch.guard).then(branch.value)
        return chain.otherwise(branches[-1].value)

    def _resolve_method(self, name: str) -> Callable:
        """Handles Python name mangling: __method → _ClassName__method."""
        if hasattr(self._instance, name):
            return getattr(self._instance, name)
        cls_name = type(self._instance).__name__.lstrip("_")
        mangled = f"_{cls_name}__{name.removeprefix('__')}"
        if hasattr(self._instance, mangled):
            return getattr(self._instance, mangled)
        raise AttributeError(
            f"Cannot resolve '{name}' on {type(self._instance).__name__}"
        )

    # ------------------------------------------------------------------ #
    #  Static value resolution (self attributes, literals)                #
    # ------------------------------------------------------------------ #

    def _self_attr(self, node: ast.Attribute) -> Any:
        parts: list[str] = []
        current: ast.expr = node
        while isinstance(current, ast.Attribute):
            parts.append(current.attr)
            current = current.value
        if not isinstance(current, ast.Name) or current.id != "self":
            raise NotImplementedError(
                f"Only self.<attr> chains are supported: {ast.unparse(node)!r}"
            )
        return reduce(getattr, reversed(parts), self._instance)

    def _static_value(self, node: ast.expr) -> Any:
        """Resolve a node to a Python value at transpile time, if possible."""
        if isinstance(node, ast.Attribute):
            return self._self_attr(node)
        try:
            return ast.literal_eval(node)
        except (ValueError, TypeError, SyntaxError, MemoryError):
            return _UNKNOWN

    # ------------------------------------------------------------------ #
    #  Function parsing                                                    #
    # ------------------------------------------------------------------ #

    @staticmethod
    def _parse_func(method: Callable) -> ast.FunctionDef:
        source = textwrap.dedent(inspect.getsource(method))
        match ast.parse(source).body[0]:
            case ast.FunctionDef() as func_def:
                return func_def
            case (
                ast.Assign(value=ast.Lambda() as lam)
                | ast.AnnAssign(value=ast.Lambda() as lam)
            ):
                return ast.FunctionDef(
                    name=getattr(method, "__name__", "<lambda>"),
                    args=lam.args,
                    body=[ast.Return(value=lam.body)],
                    decorator_list=[],
                    returns=None,
                    type_comment=None,
                )
        raise TypeError("Expected a function definition or lambda assignment")

    @classmethod
    def _param_scope(cls, func_def: ast.FunctionDef) -> _Scope:
        params = cls._params(func_def)
        return _Scope(
            columns=frozenset(param.arg for param in params),
            string_columns=frozenset(
                param.arg
                for param in params
                if cls._is_str_annotation(param.annotation)
            ),
        )

    @staticmethod
    def _params(func_def: ast.FunctionDef) -> tuple[ast.arg, ...]:
        args = (
            *func_def.args.posonlyargs,
            *func_def.args.args,
            *func_def.args.kwonlyargs,
        )
        return tuple(arg for arg in args if arg.arg != "self")

    @staticmethod
    def _is_str_annotation(annotation: ast.expr | None) -> bool:
        match annotation:
            case ast.Name(id="str") | ast.Constant(value="str"):
                return True
        return False

    @staticmethod
    def _op(table: dict[type, _BinaryOp], op: ast.AST) -> _BinaryOp:
        if (fn := table.get(type(op))) is None:
            raise NotImplementedError(
                f"Unsupported operator: {type(op).__name__}"
            )
        return fn
