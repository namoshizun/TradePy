import operator
from functools import reduce, partial
from typing import Optional as OptionalType, Union


class Clause:

    params = dict()

    def __init__(self, *args, **kwargs):
        if not self.params:
            return

        if args:
            assert len(args) == len(self.params) == 1, 'Only one query param is allowed if the clause accepts positional arguments'
            param_name = list(self.params.keys())[0]
            kwargs[param_name] = args[0]

        for name, type_and_default in self.params.items():
            expect_type = type_and_default[0]
            nullable = len(type_and_default) == 2 and type_and_default[1] is None
            is_required = len(type_and_default) == 1
            is_value_provided = name in kwargs

            if is_value_provided:
                # User provided the param value, but the type must be correct
                param_value = kwargs[name]

                if nullable and param_value is None:
                    pass
                elif not isinstance(param_value, expect_type):
                    raise TypeError(f'{param_value} is not of {expect_type} type')
            elif is_required:
                # Did not provide one, but the param is required!
                raise ValueError(f'{name} is required')
            else:
                # Not required param and user did not give one, so use the default
                param_value = type_and_default[1]

            setattr(self, name, param_value)
    

    def clone(self):
        return self.__class__(**{
            name: getattr(self, name)
            for name in self.params.keys()
        })

    def serialize(self):
        raise NotImplemented

    def chain(self, other: 'Clause') -> 'ChainedClause':
        return ChainedClause(children=[
            self, other.clone()
        ])

    def __or__(self, other: 'Clause') -> 'Clause':
        return self.chain(other)

    def __str__(self) -> str:
        return self.serialize()
    
    def __repr__(self) -> str:
        return str(self)


class ChainedClause(Clause):

    params = {
        'children': (list,)
    }

    def chain(self, other: Union['ChainedClause', Clause]) -> 'ChainedClause':
        if isinstance(other, ChainedClause):
            for child in other.children:
                self.children.append(child.clone())
        else:
            self.children.append(other.clone())

        return self

    def serialize(self):
        return '\n|> '.join(
            q.serialize()
            for q in self.children
        )


class Range(Clause):
    """
    Example:

    Range(start="-10d")
    """

    params = {
        'start': (str,),
        'stop': (str, None)
    }

    def serialize(self):
        _start = f'start: {self.start}'
        _stop = ''
        if self.stop:
            _stop = f', stop: {self.stop}'
        return f'range({_start} {_stop})'


class Filter(Clause):
    """
    Example:

    Filter('r._field == "temperature" or r._field == "humidity"')
    """

    params = {
        'expr': (str,)
    }

    def serialize(self):
        return f'filter(fn: (r) => {self.expr})'


class From(Clause):

    params = {
        'bucket': (str,)
    }

    def serialize(self):
        return f'from(bucket: "{self.bucket}")'


class Measurement(Clause):

    params = {
        'name': (str,)
    }

    def serialize(self):
        return str(Filter(f'r._measurement == "{self.name}"'))



class WithPreset:

    def __init__(self,
                 prefix: OptionalType[Clause]=None,
                 suffix: OptionalType[Clause]=None) -> None:
        self.prefix = prefix
        self.suffix = suffix

    def __call__(self, clause: Clause) -> 'Clause':
        body = []

        if isinstance(clause, ChainedClause):
            head = clause.children[0]
            assert isinstance(head, From)
            body = clause.children[1:]
        else:
            assert isinstance(clause, From)
            head = clause
        
        if self.prefix:
            body.insert(0, self.prefix)
        
        if self.suffix:
            body.append(self.suffix)

        return reduce(operator.or_, [head] + body)


def WithMeasurement(name: str):
    m_clause = Measurement(name=name)
    return WithPreset(suffix=m_clause)


# ----------
# Deprecated
class FluxQuery:

    def __init__(self, bucket: str, measurement: OptionalType[str] = None) -> None:
        self.bucket = bucket
        self.measurement = measurement
        self.clauses = []

        if measurement:
            self.filter(f'r._measurement == "{measurement}"')
    
    def clone(self):
        return FluxQuery(self.bucket, self.measurement)

    def _argument_only_query_fun(fun: str):
        def builder(self, **args):
            _args = ', '.join(
                f'{key}: {val}'
                for key, val in args.items()
            )
            template = f'{fun}({_args})'
            self.clauses.append(template)
            return self
        return builder

    movingAverage = _argument_only_query_fun('movingAverage')
    timedMovingAverage = _argument_only_query_fun('timedMovingAverage')

    def range(self, start: str, stop: str=None):
        """
        Example:

        .range(start="-10d")
        """
        _start = f'start: {start}'
        _stop = ''
        if stop:
            _stop = f', stop: {stop}'
        template = f'range({_start} {_stop})'
        self.clauses.insert(0, template)
        return self

    def filter(self, expr: str):
        """
        Example:

        .filter('r._field == "temperature" or r._field == "humidity"')
        """
        template = f'filter(fn: (r) => {expr})'
        self.clauses.append(template)
        return self
    
    def __add__(self, other: Union[str, 'FluxQuery']) -> 'FluxQuery':
        q = self.clone()

        if isinstance(other, str):
            q.flux(other)
        elif isinstance(other, FluxQuery):
            assert other.bucket == self.bucket
            assert other.measurement == self.measurement

        else:
            raise TypeError(f'Cannot add FluxQuery and {type(other)}')

        return q

    def flux(self, query):
        self.clauses.append(str(query))
        return self

    def __str__(self) -> str:
        select_bucket = f'from(bucket: "{self.bucket}")'
        return '|> '.join([
            q + '\n'
            for q in [select_bucket] + self.clauses
        ])

    def __repr__(self) -> str:
        return str(self)
