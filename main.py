import pyarrow as pa
import cppyy


cppyy.add_library_path("/home/kabbe/Code/CPP/arrow/cpp/build/install/lib64")
cppyy.load_library("arrow")

cppyy.add_include_path("/home/kabbe/Code/CPP/arrow/cpp/build/install/include/")
cppyy.include("arrow/api.h")
from cppyy.gbl import arrow as cpp_arrow


def cpp_to_py(field):
    if field.Equals(cpp_arrow.utf8()):
        return pa.utf8()
    if field.Equals(cpp_arrow.int32()):
        return pa.int32()
    if field.Equals(cpp_arrow.float64()):
        return pa.float64()
    if field.Equals(cpp_arrow.boolean()):
        return pa.boolean()
    if field.Equals(cpp_arrow.timestamp(cpp_arrow.TimeUnit.SECOND)):
        return pa.timestamp("s")


def convert(schema):
    fields = []
    for field in schema.fields():
        py_type = cpp_to_py(field.type())
        fields.append(pa.field(field.name().decode("utf-8"), py_type))
    return pa.schema(fields)


def test_cpp_to_py():
    assert cpp_to_py(cpp_arrow.utf8()) == pa.utf8()
    assert cpp_to_py(cpp_arrow.int32()) == pa.int32()
    assert cpp_to_py(cpp_arrow.timestamp(cpp_arrow.TimeUnit.SECOND)) == pa.timestamp(
        "s"
    )


def test_convert():
    schema = cpp_arrow.schema(
        [
            cpp_arrow.field("name", cpp_arrow.utf8()),
            cpp_arrow.field("age", cpp_arrow.int32()),
            cpp_arrow.field("weight", cpp_arrow.float64()),
            cpp_arrow.field("time", cpp_arrow.timestamp(cpp_arrow.TimeUnit.SECOND)),
        ]
    )
    py_schema = convert(schema)

    assert py_schema == pa.schema(
        [
            pa.field("name", pa.utf8()),
            pa.field("age", pa.int32()),
            pa.field("weight", pa.float64()),
            pa.field("time", pa.timestamp("s")),
        ]
    )
