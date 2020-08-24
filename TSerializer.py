"""
TAS TOOLS TEAM
"""
import msgpack
"""

note: <STANDARD PROCEDURE>
    1. Data is to be serialized using msgpack
    2. The packed/serialized data can be put to redis in key value pair
    3. The value from redis is fetched from key
    4. The value from redis is unpacked using msgpack
    </STANDARD PROCEDURE>
"""


"""
The following working sample illustrate only the step 1 and 4 of the STANDARD PROCEDURE.
Step2 and 3 of redis are very trivial and can be looked up on our RedisCommon.py module or on the net 
"""


'''
<<Deprecating encoding option>>

=> encoding and unicode_errors options are deprecated.

=> In case of packer, use UTF-8 always. Storing other than UTF-8 is not recommended.

=> For backward compatibility, you can use use_bin_type=False and pack bytes object into msgpack raw type.

=> In case of unpacker, there is new raw option.
    It is True by default for backward compatibility, but it is changed to False in near future.
    You can use raw=False instead of encoding='utf-8'.

<Planned backward incompatible changes>

=> When msgpack 1.0, I planning these breaking changes:

    packer and unpacker: Remove encoding and unicode_errors option.
    packer: Change default of use_bin_type option from False to True.
    unpacker: Change default of raw option from True to False.
    unpacker: Reduce all max_xxx_len options for typical usage.
    unpacker: Remove write_bytes option from all methods.

=> To avoid these breaking changes breaks your application, please:

    Don't use deprecated options.
    Pass use_bin_type and raw options explicitly.
    If your application handle large (>1MB) data, specify max_xxx_len options too.
'''

'''
<Note about performance>
   CPython's GC starts when growing allocated object. This means unpacking may cause useless GC. 
   You can use gc.disable() when unpacking large message. 
   
   List is the default sequence type of Python.
   But tuple is lighter than list. 
   You can use use_list=False while unpacking when performance is important.
'''


def tas_msgpack_pack(data):

    def tas_msgpack_encoder(obj):
        if isinstance(obj, set):
            return {
                "__type__": 'set',
                "value": list(obj)
            }
        elif isinstance(obj, tuple):
            return {
                "__type__": 'tuple',
                "value": list(obj)
            }
        return obj
    return msgpack.packb(data, default=tas_msgpack_encoder, strict_types=True, use_bin_type=False)


def tas_msgpack_unpack(packed_data):
    def tas_msgpack_decoder(obj):
        if obj.get('__type__') == 'set':
            return set(obj['value'])
        elif obj.get('__type__') == 'tuple':
            return tuple(obj['value'])
        else:
            return obj
    return msgpack.unpackb(packed_data, object_hook=tas_msgpack_decoder, raw=True)
