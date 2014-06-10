"""
"""
__version__ = '0.9.1'

__all__ = [
    'as_form',
]

import collections
import contextlib
import functools
import inspect
import weakref

from sqlalchemy import event
from sqlalchemy.orm.attributes import flag_modified
from sqlalchemy.util import memoized_property

import pilo


def as_form(attribute, form, mutable=False):
    if inspect.isclass(form) and issubclass(form, pilo.Form):
        if mutable:
            return as_mutable_form(attribute, form)
        else:
            return as_immutable_form(attribute, form)
    elif isinstance(form, pilo.Field):
        if mutable:
            return as_mutable_polymorphic_form(attribute, form)
        else:
            return as_immutable_polymorphic_form(attribute, form)
    raise ValueError(
        'Invalid form = {0}, expected form class or type field'.format(form)
    )


# internals

# https://wiki.python.org/moin/PythonDecoratorLibrary#Alternate_memoize_as_nested_functions
def memoize(func):

    cache = {}

    @functools.wraps(func)
    def memoizer(*args, **kwargs):
        key = (args, tuple(kwargs.iteritems()))
        if not isinstance(key, collections.Hashable):
            return func(*args, **kwargs)
        if key in cache:
            return cache[key]
        result = func(*args, **kwargs)
        cache[key] = result
        return result

    return memoizer


def as_immutable_form(attribute, form_cls):

    def _coerce(value):
        return form_cls(value)

    return CoerceForm(attribute, form_cls, _coerce)


def as_immutable_polymorphic_form(attribute, field):

    def _coerce(value):
        identity = field.probe(value)
        if identity not in field.types:
            raise ValueError(
                'Invalid value for field {0} = {1}'.format(field, identity)
            )
        return field.types[identity](value)

    return CoerceForm(attribute, field.parent, _coerce)


def as_mutable_form(attribute, form_cls):

    def _coerce(value):
        return form_cls(value)

    return CoerceMutableForm(attribute, form_cls, _coerce)


def as_mutable_polymorphic_form(attribute, field):

    def _coerce(value):
        identity = field.probe(value)
        if identity not in field.types:
            raise ValueError(
                'Invalid value for field {0} = {1}'.format(field, identity)
            )
        return field.types[identity](value)

    return CoerceMutableForm(attribute, field.parent, _coerce)



class CoerceForm(object):

    def __init__(self, attribute, form_cls, coerce):
        self.form_cls = form_cls
        self.coerce = coerce
        self.key = attribute.key
        parent_cls = attribute.class_
        event.listen(parent_cls, 'load', self.load, raw=True, propagate=True)
        event.listen(parent_cls, 'refresh', self.refresh, raw=True, propagate=True)
        event.listen(attribute, 'set', self.set, raw=True, retval=True, propagate=True)

    def load(self, state, *args):
        val = state.dict.get(self.key, None)
        if val is not None:
            val = self._as_form(val)
            state.dict[self.key] = val

    def refresh(self, state, *args):
        return self.load(state, *args)

    def set(self, target, value, oldvalue, initiator):
        if value is oldvalue:
            return value
        return self._as_form(value)

    def _as_form(self, value):
        if value is None:
            return value
        if isinstance(value, self.form_cls):
            return value
        return self.coerce(value)


class CoerceMutableForm(object):

    form_clses = (pilo.Form,)

    def __init__(self, attribute, form_cls, coerce):
        self.form_cls = form_cls
        self.coerce = coerce
        self.key = attribute.key
        parent_cls = attribute.class_
        event.listen(parent_cls, 'load', self.load, raw=True, propagate=True)
        event.listen(parent_cls, 'refresh', self.refresh, raw=True, propagate=True)
        event.listen(attribute, 'set', self.set, raw=True, retval=True, propagate=True)
        event.listen(parent_cls, 'pickle', self.pickle, raw=True, propagate=True)
        event.listen(parent_cls, 'unpickle', self.unpickle, raw=True, propagate=True)

    def load(self, state, *args):
        val = state.dict.get(self.key, None)
        if val is not None:
            val = self._as_sink(self._as_form(val))
            state.dict[self.key] = val
            val._parents[state.obj()] = self.key

    def refresh(self, state, *args):
        return self.load(state, *args)

    def set(self, target, value, oldvalue, initiator):
        if value is oldvalue:
            return value
        if not isinstance(value, self.form_cls):
            value = self._as_form(value)
        if not isinstance(value, MutableSink):
            value = self._as_sink(value)
        if value is not None:
            value._parents[target.obj()] = self.key
        if isinstance(oldvalue, MutableSink):
            oldvalue._parents.pop(target.obj(), None)
        return value

    def pickle(self, state, state_dict):
        val = state.dict.get(self.key, None)
        if val is not None:
            if 'fical.mutable.values' not in state_dict:
                state_dict['fical.mutable.values'] = []
            state_dict['fical.mutable.values'].append(val)

    def unpickle(self, state, state_dict):
        if 'fical.mutable.values' in state_dict:
            for val in state_dict['fical.mutable.values']:
                val._parents[state.obj()] = self.key

    def _as_sink(self, value):
        if value is None:
            return value
        if not isinstance(value, self.form_cls):
            value = self._as_form(value)
        if isinstance(value, MutableSink):
            return value
        sink = mutable_sink(type(value))()
        for k, v in value.iteritems():
            sink[k] = self._as_mutable_source(v, sink)
        return sink

    def _as_form(self, value):
        if value is None:
            return value
        if isinstance(value, self.form_cls):
            return value
        return self.coerce(value)

    def _as_mutable_source(self, v, sink):
        if isinstance(v, self.form_clses):
            return self._as_mutable_form(v, sink)
        if isinstance(v, dict):
            return self._as_mutable_dict(v, sink)
        if isinstance(v, list):
            return self._as_mutable_list(v, sink)
        return v

    def _as_mutable_form(self, value, sink):
        mv = mutable_form(type(value))(value)
        for k, v in value.iteritems():
            mv[k] = self._as_mutable_source(v, sink)
        mv._attach(sink)
        return mv

    def _as_mutable_dict(self, value, sink):
        mv = MutableDict()
        for k, v in value.iteritems():
            mv[k] = self._as_mutable_source(v, sink)
        mv._attach(sink)
        return mv

    def _as_mutable_list(self, value, sink):
        mv = MutableList()
        for v in value:
            mv.append(self._as_mutable_source(v, sink))
        mv._attach(sink)
        return mv


class MutableSink(object):

    @memoized_property
    def _parents(self):
        return weakref.WeakKeyDictionary()

    def _attach(self, parents):
        self.parents.update(parents)

    def _detach(self):
        parents = self._parents.copy()
        self._parent.clear()
        return parents

    @contextlib.contextmanager
    def _disable(self):
        parents = self._detach()
        try:
            yield
        finally:
            self._attach(parents)

    def _changed(self):
        for parent, key in self._parents.items():
            flag_modified(parent, key)


@memoize
def mutable_sink(form_cls):
    return type(
        'Mutable' + form_cls.__name__ + 'Sink',
        (MutableDictMixin, form_cls, MutableSink),
        {
            '__module__': form_cls.__module__
        },
    )


class MutableSource(object):

    _mutable_sink = None

    def _attach(self, sink):
        self._mutable_sink = weakref.proxy(sink)

    def _detach(self):
        self._mutable_sink = None

    def _changed(self):
        if self._mutable_sink:
            self._mutable_sink._changed()


class MutableDictMixin(object):

    def __setitem__(self, key, value):
        dict.__setitem__(self, key, value)
        self._changed()

    def __delitem__(self, key):
        dict.__delitem__(self, key)
        self._changed()

    def clear(self):
        dict.clear(self)
        self._changed()

    def update(self, other):
        dict.update(self, other)
        self._changed()


class MutableDict(MutableDictMixin, dict, MutableSource):

    pass


class MutableListMixin(object):

    def append(self, *args, **kwargs):
        list.append(self, *args, **kwargs)
        self._changed()

    def insert(self, *args, **kwargs):
        list.insert(self, *args, **kwargs)
        self._changed()

    def extend(self, *args, **kwargs):
        list.extend(self, *args, **kwargs)
        self._changed()

    def remove(self, *args, **kwargs):
        list.remove(self, *args, **kwargs)
        self._changed()

    def pop(self, *args, **kwargs):
        list.pop(self, *args, **kwargs)
        self._changed()


class MutableList(MutableListMixin, list, MutableSource):

    pass


@memoize
def mutable_form(form_cls):
    return type(
        'Mutable' + form_cls.__name__,
        (MutableDictMixin, form_cls, MutableSource),
        {
            '__module__': form_cls.__module__
        },
    )
