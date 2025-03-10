# BSD 3-Clause License
#
# Copyright (c) 2021-2024, Massachusetts Institute of Technology.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this
#    list of conditions and the following disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice,
#    this list of conditions and the following disclaimer in the documentation
#    and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its
#    contributors may be used to endorse or promote products derived from
#    this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
"""Base config class for config structures."""

import copy
import dataclasses
import inspect
import logging
import pathlib
import pprint
import typing

import yaml

Logger = logging.getLogger(__name__)


class ConfigTypeParser:
    """Parser for non-config types."""

    __shared_state = None

    def __init__(self):
        """Make a factory."""
        # Borg pattern: set class state to global state
        if not ConfigTypeParser.__shared_state:
            ConfigTypeParser.__shared_state = self.__dict__
            self._registry = {}
        else:
            self.__dict__ = ConfigTypeParser.__shared_state

    @staticmethod
    def register(field_type, parser_func):
        """
        Register a custom parser for a specific type.

        Parsers have the following signature:
            _(field_type, field: dataclasses.field, value: Any, strict: bool)

        They convert `value` (parsed from YAML) to the field type, optionally
        respecting the `strict` argument. For numeric types, this usually indicates
        whether or not to try constructing the type from the YAML value.

        Args:
            field_type: Type info of desired value
            parser_func: Function that takes in type info, field description and yaml value
        """
        ConfigTypeParser()._registry[field_type] = parser_func

    @staticmethod
    def parse(field_type, field, value, strict=True):
        """Parse the value for a field from yaml (for internal use)."""
        instance = ConfigTypeParser()
        if field_type not in instance._registry:
            return value

        return instance._registry[field_type](field_type, field, value, strict=strict)


def register_type_parser(func):
    """
    Register a conversion for a non-config type.

    Can be used as a decorator.
    """
    try:
        param = next(iter(inspect.signature(func).parameters.values()))
        ConfigTypeParser.register(param.annotation, func)
    except Exception as e:
        logging.error(f"Invalid parser: {e}!")
        return func

    return func


@register_type_parser
def _(field_type: int, field, value, strict=True):
    return int(value) if strict else value


@register_type_parser
def _(field_type: float, field, value, strict=True):
    return float(value) if strict else value


@register_type_parser
def _(field_type: str, field, value, strict=True):
    return str(value) if strict else value


def _is_config_list(list_type):
    return issubclass(typing.get_args(list_type)[0], Config)


def _sequence_iter(yaml_field):
    if isinstance(yaml_field, list):
        for x in yaml_field:
            yield x
    elif isinstance(yaml_field, dict):
        for _, v in yaml_field.items():
            yield v


class Config:
    """Base class for any config."""

    def dump(self, add_type=False):
        """Dump a config to a yaml-compatible dictionary."""
        values = dataclasses.asdict(self)
        info = ConfigFactory.get_info(self)
        if add_type and info:
            values["type"] = info[1]

        for field in dataclasses.fields(self):
            field_value = getattr(self, field.name)
            if field.metadata.get("virtual_config", False):
                need_type = not isinstance(field_value, VirtualConfig)
                values[field.name] = field_value.dump(add_type=need_type)
            elif isinstance(field_value, Config):
                # recursion required because as_dict won't dispatch dump
                values[field.name] = field_value.dump()

        return values

    def save(self, filepath):
        """Dump a class to disk."""
        filepath = pathlib.Path(filepath)
        with filepath.open("w") as fout:
            fout.write(yaml.safe_dump(self.dump()))

    def update(self, config, strict=True, warn_missing=False, _parent=""):
        """
        Load settings from a dumped config.

        When parsing a field that is a list of configs, the parsed list will override
        what was already set for that field if the field is specified in the parameter tree.
        This allows clearing a default list with elements by specifying an empty list in yaml.

        Args:
            config (dict[Any, Any]): Parsed YAML parameter tree representation
            strict (bool): Enforce dataclass types when parsing
            warn_missing (bool): Warn if YAML parameter is missing
        """
        if not isinstance(config, dict):
            Logger.error(f"Invalid config data provided to {self}")
            return

        for field in dataclasses.fields(self):
            global_name = _parent + "/" + field.name if _parent != "" else field.name
            if field.name not in config:
                if warn_missing:
                    Logger.warning(f"Missing {global_name} when parsing config!")

                continue

            prev = getattr(self, field.name)
            field_config = config[field.name]
            # NOTE(nathan) issubclass(field.type, Config) sorta works here but generics get complicated
            if isinstance(prev, Config) or field.metadata.get("virtual_config", False):
                prev.update(
                    field_config,
                    strict=strict,
                    warn_missing=warn_missing,
                    _parent=global_name,
                )
            elif isinstance(prev, list) and _is_config_list(field.type):
                self._parse_yaml_list(
                    field,
                    field_config,
                    global_name,
                    strict=strict,
                    warn_missing=warn_missing,
                )
            else:
                self._parse_yaml_leaf(field, field_config, global_name, strict=strict)

    def show(self):
        """Show config in human readable format."""
        return f"{self.__class__.__name__}:\n{pprint.pformat(self.dump())}"

    @staticmethod
    def load(cls, filepath, strict=True, warn_missing=False):
        """Load an abitrary config from file."""
        assert issubclass(cls, Config), f"{cls} is not a config!"

        instance = cls()
        with pathlib.Path(filepath).open("r") as fin:
            instance.update(
                yaml.safe_load(fin), strict=strict, warn_missing=warn_missing
            )

        return instance

    def _parse_yaml_leaf(self, field, field_config, global_name, strict=True):
        try:
            value = field_config
            if "yaml_converter" in field.metadata:
                value = field.metadata["yaml_converter"](value)
            elif strict:
                value = ConfigTypeParser.parse(field.type, field, value, strict=strict)

            setattr(self, field.name, value)
        except Exception as e:
            field_str = f"{type(self)} at '{global_name}' ({field.type})"
            Logger.error(f"Skipping invalid YAML when parsing {field_str}: {e}")

    def _parse_yaml_list(self, field, field_config, global_name, **kwargs):
        parsed = []
        for idx, subconfig in enumerate(_sequence_iter(field_config)):
            curr_name = global_name + f"[{idx}]"
            try:
                subfield = typing.get_args(field.type)[0]()
            except Exception as e:
                Logger.error(f"Could not init {field.type} at '{global_name}': {e}")
                break

            subfield.update(subconfig, _parent=curr_name, **kwargs)
            parsed.append(subfield)

        setattr(self, field.name, parsed)


class ConfigFactory:
    """Factory for configs."""

    __shared_state = None

    def __init__(self):
        """Make a factory."""
        # Borg pattern: set class state to global state
        if not ConfigFactory.__shared_state:
            ConfigFactory.__shared_state = self.__dict__
            self._factories = {}
            self._lookup = {}
            self._constructors = {}
        else:
            self.__dict__ = ConfigFactory.__shared_state

    @staticmethod
    def create(category, name, *args, **kwargs):
        """Instantiate a specific config."""
        instance = ConfigFactory()
        if category not in instance._factories:
            Logger.warning(f"No configs registered under category '{category}'!")
            return None

        category_factories = instance._factories[category]
        if name not in category_factories:
            Logger.error(f"Config '{name}' not registered under category '{category}'!")
            return None

        return category_factories[name](*args, **kwargs)

    @staticmethod
    def register(config_type, category, name=None, constructor=None):
        """Register a config type with the factory."""
        instance = ConfigFactory()
        if category not in instance._factories:
            instance._factories[category] = {}

        name = name if name else config_type.__name__
        instance._factories[category][name] = config_type
        instance._lookup[str(config_type)] = (category, name)
        if constructor:
            if category not in instance._constructors:
                instance._constructors[category] = {}
            instance._constructors[category][str(config_type)] = constructor

    @staticmethod
    def registered():
        """Get all registered types."""
        factories = ConfigFactory()._factories
        return {c: [(n, t) for n, t in fact.items()] for c, fact in factories.items()}

    @staticmethod
    def get_info(cls_instance):
        """Lookup category and name for class type."""
        typename = str(type(cls_instance))
        instance = ConfigFactory()
        return instance._lookup.get(typename)

    @staticmethod
    def get_constructor(category, name):
        """Get a constructor for a type if it exists."""
        instance = ConfigFactory()
        if category not in instance._factories:
            return None

        category_factories = instance._factories[category]
        if name not in category_factories:
            return None

        typename = str(category_factories[name])
        return instance._constructors[category][typename]


class VirtualConfig:
    """Holder for config type."""

    def __init__(self, category, default=None, required=True):
        """Make a virtual config."""
        self.category = category
        self.required = required
        self._type = default
        self._config = None

    def dump(self, **kwargs):
        """Dump underlying config."""
        if not self._config:
            self._create(validate=False)
            if not self._config:
                # return {}
                return None

        # TODO(nathan) this is janky
        values = self._config.dump()
        values["type"] = self._type
        return values

    def update(self, config_data, strict=True, warn_missing=False, _parent=""):
        """
        Update virtual config from parsed parameters.

        Will reset the underlying config if `type: CONFIG_TYPE` is in the parameter
        tree.

        Args:
            config_data (dict[Any, Any]): Parsed YAML parameter tree representation
            strict: Enforce dataclass types when parsing
            warn_missing: Warn if fields aren't present in YAML
        """

        if config_data is not None and not isinstance(config_data, dict):
            raise ValueError(
                f"VirtualConfig must be updated by Optional[dict], not {type(config_data)}. You probably nested your configuration wrong"
            )

        typename = config_data.get("type", self._type)
        if typename is None and self.required:
            Logger.error(f"Could not get type for {self} from '{config_data}'!")

        type_changed = typename is not None and self._type != typename
        if not self._config or type_changed:
            self._create(typename=typename)

        if self._config is not None and config_data != {}:
            self._config.update(
                config_data, strict=strict, warn_missing=warn_missing, _parent=_parent
            )

        return self

    def create(self, *args, **kwargs):
        """Call constructor with config."""
        name = self._get_curr_typename()
        constructor = ConfigFactory.get_constructor(self.category, name)
        if constructor is None:
            if not self.required:
                Logger.warning(f"Constructor not specified for {self}!")
                return None

            raise ValueError(f"No constructor found for '{self.category}' and '{name}'")

        if not self._config:
            self._create()

        return constructor(self._config, *args, **kwargs)

    def _config_str(self):
        return f"<VirtualConfig(category={self.category}, type='{self._type}')>"

    def _get_curr_typename(self):
        return self._type

    def _create(self, typename=None, validate=True):
        name = typename if typename else self._get_curr_typename()
        if name is None and (self.required and validate):
            raise ValueError(f"Could not make virtual config for '{self.category}'")

        self._config = ConfigFactory.create(self.category, name)
        self._type = name

    def __repr__(self):
        """Access underlying repr."""
        return self._config_str() if not self._config else self._config.__repr__()

    def __eq__(self, other):
        """Access underlying eq."""
        return self._config == other if self._config else False

    def __deepcopy__(self, memo):
        """Copy virtual config."""
        new_config = VirtualConfig(self.category, required=self.required)
        new_config._type = self._type
        if self._config:
            new_config._config = copy.deepcopy(self._config, memo)

        return new_config

    def __getattr__(self, name):
        """Get underlying config field."""
        # TODO(nathan) clean this up
        assert name not in ["category", "_type", "_config"]

        if not self._config:
            # create config if it doesn't exist
            self._create()

        if self._config is None:
            raise ValueError(f"Uninitialized config: {self}!")

        return getattr(self._config, name)


def register_config(category, name="", constructor=None):
    """Register a class with the factory."""

    def decorator(cls):
        ConfigFactory.register(cls, category, name=name, constructor=constructor)
        return cls

    return decorator


def config_field(category, default=None, required=True):
    """Return a dataclass field."""

    def factory():
        return VirtualConfig(category, default=default, required=required)

    return dataclasses.field(default_factory=factory, metadata={"virtual_config": True})
