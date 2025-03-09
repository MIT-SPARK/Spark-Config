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
"""Test that configuration structs work as expected."""

from dataclasses import dataclass, field
from typing import Any, List
import yaml

import spark_config as sc
import logging


@sc.register_config("test", name="foo")
@dataclass
class Foo(sc.Config):
    """Test configuration struct."""

    a: float = 5.0
    b: int = 2
    c: str = "hello"


@sc.register_config("test", name="bar")
@dataclass
class Bar(sc.Config):
    """Test configuration struct."""

    bar: str = "world"
    d: int = 15


@dataclass
class Parent(sc.Config):
    """Test configuration struct."""

    child: Any = sc.config_field("test", default="foo")
    param: float = -1.0


@dataclass
class NestedConfig(sc.Config):
    """Test configuration struct."""

    foo: Foo = field(default_factory=Foo)
    other: str = "world"


@dataclass
class ListConfig(sc.Config):
    children: List[Foo] = field(default_factory=list)


def test_dump():
    """Make sure dumping works as expected."""
    foo = Foo()
    result = foo.dump()
    expected = {"a": 5.0, "b": 2, "c": "hello"}
    assert result == expected

    bar = Bar()
    result = bar.dump()
    expected = {"bar": "world", "d": 15}
    assert result == expected

    parent = Parent()
    result = parent.dump()
    expected = {"child": {"type": "foo", "a": 5.0, "b": 2, "c": "hello"}, "param": -1.0}
    assert result == expected


def test_update():
    """Test that update works as expected."""
    foo = Foo()
    assert foo == Foo()

    # empty update does nothing
    foo.update({})
    assert foo == Foo()

    # non-dict update does nothing
    foo.update(5.0)
    assert foo == Foo()

    foo.update({"a": 10.0})
    assert foo == Foo(a=10.0)

    foo.update({"b": 1, "c": "world"})
    assert foo == Foo(a=10.0, b=1, c="world")

    parent = Parent()
    parent.update({"child": {"b": 1, "c": "world"}, "param": -2.0})
    assert parent == Parent(child=Foo(a=5.0, b=1, c="world"), param=-2.0)


def test_update_recursive():
    """Test that update recurses to non-virtual configs."""
    nested = NestedConfig()
    assert nested == NestedConfig()

    # empty update does nothing
    nested.update({})
    assert nested == NestedConfig()

    nested.update({"foo": {"b": 1, "c": "world"}, "other": "hello!"})
    assert nested == NestedConfig(foo=Foo(a=5.0, b=1, c="world"), other="hello!")


def test_dump_recursive():
    """Test that dump recurses to non-virtual configs."""
    nested = NestedConfig()
    expected = {"foo": {"a": 5.0, "b": 2, "c": "hello"}, "other": "world"}
    assert nested.dump() == expected


def test_save_load(tmp_path):
    """Test that saving and loading works."""
    filepath = tmp_path / "config.yaml"
    parent = Parent(child=Bar(bar="hello", d=2.0), param=-2.0)
    parent.save(filepath)
    result = sc.Config.load(Parent, filepath)
    assert parent == result


def test_show():
    """Test that show looks sane."""
    foo = Foo()
    expected = "Foo:\n{'a': 5.0, 'b': 2, 'c': 'hello'}"
    assert foo.show() == expected


def test_factory():
    """Test that the factory works."""
    registered = sc.ConfigFactory.registered()
    assert len(registered) > 0
    assert "test" in registered
    registered_names = [x[0] for x in registered["test"]]
    assert "foo" in registered_names
    assert "bar" in registered_names


def test_config_list():
    """Test that loading lists of configs works."""
    contents = """
children:
- {a: 1, b: 2, c: 3}
- {a: 2, b: 3, c: 4}
- {}
"""
    config = ListConfig()
    config.update(yaml.safe_load(contents))
    assert config.children == [Foo(a=1.0, b=2, c="3"), Foo(a=2.0, b=3, c="4"), Foo()]


def test_config_list_from_map():
    """Test that loading lists of configs from YAML map works."""
    contents = """
children:
  child_a: {a: 1, b: 2, c: 3}
  child_b: {a: 2, b: 3, c: 4}
  child_c: {}
"""

    config = ListConfig()
    config.update(yaml.safe_load(contents))
    assert config.children == [Foo(a=1.0, b=2, c="3"), Foo(a=2.0, b=3, c="4"), Foo()]


def test_config_override():
    """Test that we can clear default lists."""
    config = ListConfig()
    config.children = [Foo(a=1.0, b=2, c="3"), Foo(a=2.0, b=3, c="4"), Foo()]

    # children not set: keep previous values
    config.update({})
    assert config.children == [Foo(a=1.0, b=2, c="3"), Foo(a=2.0, b=3, c="4"), Foo()]

    # children set: clear previous values
    config.update({"children": []})
    assert config.children == []


def test_invalid_field(caplog):
    """Test that invalid types don't get set with strict parsing."""
    config = Bar()
    with caplog.at_level(logging.ERROR, logger=sc.Logger.name):
        sc.Logger.propagate = True

        config.update({"bar": "test", "d": "2.0"})
        assert config == Bar(bar="test")
        config.update({"bar": "test", "d": "2.0"}, strict=False)
        assert config == Bar(bar="test", d="2.0")

    assert len(caplog.records) == 1


def test_unregistered(caplog):
    """Test that factory handles unregistered types correctly."""
    with caplog.at_level(logging.WARNING, logger=sc.Logger.name):
        sc.Logger.propagate = True
        assert sc.ConfigFactory.create("some_category", "some_type") is None
        assert sc.ConfigFactory.create("test", "some_type") is None

    assert len(caplog.records) == 2
