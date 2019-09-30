# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: kstpohlc.proto

import sys
_b=sys.version_info[0]<3 and (lambda x:x) or (lambda x:x.encode('latin1'))
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='kstpohlc.proto',
  package='stocktrading.price',
  syntax='proto3',
  serialized_options=None,
  serialized_pb=_b('\n\x0ekstpohlc.proto\x12\x12stocktrading.price\"\xa9\x01\n\x08KSTPrice\x12\n\n\x02\x64t\x18\x01 \x01(\t\x12\x0e\n\x06market\x18\x02 \x01(\t\x12\x0c\n\x04\x63ode\x18\x03 \x01(\t\x12\x0e\n\x06ticker\x18\x04 \x01(\t\x12\x0c\n\x04name\x18\x05 \x01(\t\x12\x0c\n\x04open\x18\x06 \x01(\x02\x12\x0c\n\x04high\x18\x07 \x01(\x02\x12\x0b\n\x03low\x18\x08 \x01(\x02\x12\r\n\x05\x63lose\x18\t \x01(\x02\x12\x0e\n\x06volume\x18\n \x01(\x02\x12\r\n\x05value\x18\x0b \x01(\x02\x62\x06proto3')
)




_KSTPRICE = _descriptor.Descriptor(
  name='KSTPrice',
  full_name='stocktrading.price.KSTPrice',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  fields=[
    _descriptor.FieldDescriptor(
      name='dt', full_name='stocktrading.price.KSTPrice.dt', index=0,
      number=1, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='market', full_name='stocktrading.price.KSTPrice.market', index=1,
      number=2, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='code', full_name='stocktrading.price.KSTPrice.code', index=2,
      number=3, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='ticker', full_name='stocktrading.price.KSTPrice.ticker', index=3,
      number=4, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='name', full_name='stocktrading.price.KSTPrice.name', index=4,
      number=5, type=9, cpp_type=9, label=1,
      has_default_value=False, default_value=_b("").decode('utf-8'),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='open', full_name='stocktrading.price.KSTPrice.open', index=5,
      number=6, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='high', full_name='stocktrading.price.KSTPrice.high', index=6,
      number=7, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='low', full_name='stocktrading.price.KSTPrice.low', index=7,
      number=8, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='close', full_name='stocktrading.price.KSTPrice.close', index=8,
      number=9, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='volume', full_name='stocktrading.price.KSTPrice.volume', index=9,
      number=10, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
    _descriptor.FieldDescriptor(
      name='value', full_name='stocktrading.price.KSTPrice.value', index=10,
      number=11, type=2, cpp_type=6, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=39,
  serialized_end=208,
)

DESCRIPTOR.message_types_by_name['KSTPrice'] = _KSTPRICE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

KSTPrice = _reflection.GeneratedProtocolMessageType('KSTPrice', (_message.Message,), dict(
  DESCRIPTOR = _KSTPRICE,
  __module__ = 'kstpohlc_pb2'
  # @@protoc_insertion_point(class_scope:stocktrading.price.KSTPrice)
  ))
_sym_db.RegisterMessage(KSTPrice)


# @@protoc_insertion_point(module_scope)