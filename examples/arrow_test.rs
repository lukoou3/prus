use std::sync::Arc;
use arrow::compute::kernels::numeric::add_wrapping;
use arrow::compute::kernels::substring::substring_by_char;
use arrow::datatypes::{DataType, Field, Fields};
use arrow_array::{record_batch, Array, ArrayRef, BooleanArray, GenericListArray, Int32Array, PrimitiveArray, RecordBatch, StringArray, StructArray};
use arrow_array::builder::{BooleanBuilder, Int32Builder, ListBuilder, StringBuilder, StructBuilder};
use arrow_array::cast::AsArray;
use arrow_array::types::Int32Type;
use arrow_schema::Schema;

fn test_primitive_array() {
    let int_array1: PrimitiveArray<Int32Type> = Int32Array::from(vec![1, 2, 3, 4]);
    let int_array2 = Int32Array::from(vec![Some(1), None, Some(3), Some(4)]);
    let int_array3 = Int32Array::from_iter([1, 2, 3, 4]);
    let int_array4 = Int32Array::from_iter([Some(1), Some(2), Some(3), Some(4)]); // 如果数据中没有null，最终build时会过滤掉，nulls标记为None
    // Create a Int32Array from Vec without copying
    let array1 = Int32Array::new(vec![1, 2, 3, 4].into(), None); // 比较高效，vec中的数据直接转移了

    // Create a new builder with a capacity of 100
    let mut builder = Int32Array::builder(10); // 最常用的方式，也是比较高效的，null_buffer_builder默认是None，只有添加null时才会创建

    // Append a single primitive value
    builder.append_value(1);
    // Append a null value
    builder.append_null();
    // Append a slice of primitive values
    builder.append_slice(&[3, 4]);

    // Build the array
    let array2 = builder.finish();

    let mut builder = Int32Builder::with_capacity(10);;
    builder.append_value(1);
    builder.append_value(2);
    builder.append_value(3);
    builder.append_value(4);
    let array3 = builder.finish();

    for (i, array) in [int_array1, int_array2, int_array3, int_array4, array1, array2, array3].into_iter().enumerate() {
        println!("{} array:{:?}", i + 1, array);
        let nulls = array.nulls();
        println!("nulls:{}-{:?}", nulls.is_some(), nulls.map(|x| x.inner().values()).unwrap_or_default().iter().map(|x| format!("{:b}", x)).collect::<Vec<_>>());
        let data: Vec<_> = array.iter().collect();
        println!("data:{:?}", data);
        println!("{}", "*".repeat(50))
    }


}

fn test_to_vec() {
    let arr = PrimitiveArray::<Int32Type>::from(vec![1, 2, 3, 4]);
    let starting_ptr = arr.values().as_ptr();
    // split into its parts。会消耗所有权
    let (datatype, buffer, nulls) = arr.into_parts();
    // Convert the buffer to a Vec<i32> (zero copy)
    // (note this requires that there are no other references)
    let mut vec: Vec<i32> = buffer.into();
    vec[2] = 300;
    // put the parts back together
    let arr = PrimitiveArray::<Int32Type>::try_new(vec.into(), nulls).unwrap();
    assert_eq!(arr.values(), &[1, 2, 300, 4]);
    // The same allocation was used
    assert_eq!(starting_ptr, arr.values().as_ptr());
}

fn test_array_ref_cast() {
    // array通常作为动态类型的&dyn Array或ArrayRef传递。RecordBatch将列存储为ArrayRef。
    // To get an ArrayRef, wrap the Int32Array in an Arc.
    // (note you will often have to explicitly type annotate to ArrayRef)
    let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));

    // Downcast to Int32Array using as_any
    let int_array1 = arr.as_any().downcast_ref::<Int32Array>().unwrap();
    // This is the same as using the as_::<T>() helper
    let int_array2 = arr.as_primitive::<Int32Type>();
    assert_eq!(int_array1, int_array2);

    println!("{}-{:?}", arr.data_type(), int_array1.data_type());
    println!("{:?}", int_array1);
    println!("{:?}", int_array2);
}

fn test_list_array() {
    let mut a = ListBuilder::new(Int32Builder::new());
    // [1, 2]
    a.values().append_value(1);
    a.values().append_value(2);
    a.append(true);
    // null
    a.append(false);
    // []
    a.append(true);
    // [3, null]
    a.values().append_value(3);
    a.values().append_null();
    a.append(true);

    // [[1, 2], null, [], [3, null]]
    let a: GenericListArray<i32> = a.finish();

    let data: Vec<_> = a.iter().map(|x| x.map(|x| x.as_primitive::<Int32Type>().iter().collect::<Vec<_>>())).collect();
    println!("{:?}", a);
    println!("{:?}", data);
}

fn test_struct_array() {
    let boolean = Arc::new(BooleanArray::from(vec![false, false, true, true]));
    let int = Arc::new(Int32Array::from(vec![42, 28, 19, 31]));

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("b", DataType::Boolean, false)),
            boolean.clone() as ArrayRef,
        ),
        (
            Arc::new(Field::new("c", DataType::Int32, false)),
            int.clone() as ArrayRef,
        ),
    ]);

    let fields = vec![Field::new("b", DataType::Boolean, false), Field::new("c", DataType::Int32, true)];
    let struct_array2 = StructArray::new(Fields::from(fields), vec![boolean, int], None);

    println!("{:?}", struct_array);
    println!("{:?}", struct_array2);

}

fn test_struct_array_build() {
    let fields = Fields::from(vec![
        Field::new("id", DataType::Int32, true),
        Field::new("name", DataType::Utf8, true),
        Field::new("active", DataType::Boolean, false),
    ]);

    let mut struct_builder = StructBuilder::new(
        fields,
        vec![
            Box::new(Int32Builder::with_capacity(10)),
            Box::new(StringBuilder::with_capacity(10, 200)),
            Box::new(BooleanBuilder::with_capacity(10)),
        ],
    );

    // 第1行：正常值
    {
        let id_builder = struct_builder.field_builder::<Int32Builder>(0).unwrap();
        id_builder.append_value(1);

        let name_builder = struct_builder.field_builder::<StringBuilder>(1).unwrap();
        name_builder.append_value("Alice");

        let active_builder = struct_builder.field_builder::<BooleanBuilder>(2).unwrap();
        active_builder.append_value(true);

        struct_builder.append(true);   // true 表示这一行 struct 是 non-null
    }

    // 第2行：整个 struct 为 null（推荐写法）
    {
        // 必须给每个子字段 append_null（即使 struct 是 null）
        struct_builder.field_builder::<Int32Builder>(0).unwrap().append_value(0);
        struct_builder.field_builder::<StringBuilder>(1).unwrap().append_value("");
        struct_builder.field_builder::<BooleanBuilder>(2).unwrap().append_value(false); // 虽然字段不可空，但这里仍需 append
        struct_builder.append(false);  // 或 struct_builder.append_null();
    }

    // 第3行：部分字段为 null（id 和 name 为 null，active 必须有值）
    {
        let id_builder = struct_builder.field_builder::<Int32Builder>(0).unwrap();
        id_builder.append_null();

        let name_builder = struct_builder.field_builder::<StringBuilder>(1).unwrap();
        name_builder.append_null();

        let active_builder = struct_builder.field_builder::<BooleanBuilder>(2).unwrap();
        active_builder.append_value(false);

        struct_builder.append(true);   // struct 本身 non-null，但里面有 null 字段
    }

    // 第4行：再加一行
    {
        struct_builder.field_builder::<Int32Builder>(0).unwrap().append_value(42);
        struct_builder.field_builder::<StringBuilder>(1).unwrap().append_value("Bob");
        struct_builder.field_builder::<BooleanBuilder>(2).unwrap().append_value(true);
        struct_builder.append(true);
    }

    let array = struct_builder.finish();
    println!("{:?}", array);
}

fn test_record_batch() {
    let batch = record_batch!(
        ("a", Int32, [1, 2, 3]),
        ("b", Float64, [Some(4.0), None, Some(5.0)]),
        ("c", Utf8, ["alpha", "beta", "gamma"])
    ).unwrap();
    println!("{:?}", batch);


    let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int32, false)
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(id_array)]
    ).unwrap();
    println!("{:?}", batch);
}

fn test_add_function() {
    let arr1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let arr2: ArrayRef = Arc::new(Int32Array::from(vec![4, 5, 6]));

    // 直接使用内置的函数
    let result = add_wrapping(&arr1, &arr2).unwrap();
    println!("{:?}", result);

    // 自己实现
    let int_array2 = arr1.as_primitive::<Int32Type>();
    let int_array3 = arr2.as_primitive::<Int32Type>();
    let mut builder = Int32Array::builder(10);
    for (x, y) in int_array2.iter().zip(int_array3.iter()) {
        if let (Some(x), Some(y)) = (x, y) {
            builder.append_value(x + y);
        } else {
            builder.append_null();
        }
    }
    let result = builder.finish();
    println!("{:?}", result);

}

fn test_substr_function() {
    let arr1: ArrayRef = Arc::new(StringArray::from(vec![Some("12345"), None, Some("12"), Some("1"), Some("一二三四五")]));
    let str_array1 = arr1.as_string::<i32>();

    // 直接使用内置的函数
    let result = substring_by_char(&str_array1, 1, Some(2)).unwrap();
    println!("{:?}", result);

    // 自己实现

    let mut builder = StringBuilder::new();
    for x in str_array1.iter() {
        if let Some(s) = x {
            builder.append_value(s.chars().into_iter().skip(1).take(2).collect::<String>());
        } else {
            builder.append_null();
        }
    }
    let result = builder.finish();
    println!("{:?}", result);

}

fn test_split_function() {
    let arr: ArrayRef = Arc::new(StringArray::from(vec![Some("12,34,5"), None, Some("12"), Some("一二三,四五")]));
    let str_array = arr.as_string::<i32>();

    let string_builder = StringBuilder::with_capacity(str_array.len(), str_array.get_buffer_memory_size());
    let mut list_builder = ListBuilder::new(string_builder);
    for string in str_array.iter() {
        if let Some(s) = string {
            for x in s.split(',') {
                list_builder.values().append_value(x);
            }
            list_builder.append(true);
        } else {
            list_builder.append(false);
        }
    }
    let list_array = list_builder.finish();
    println!("{:?}", list_array);
    let data: Vec<_> = list_array.iter().map(|o| o.map(|x| x.as_string::<i32>().iter().map(|x| x.unwrap_or_default().to_string()).collect::<Vec<_>>())).collect();
    println!("{:?}", data);
}


fn main() {
    //test_primitive_array();
    //test_array_ref_cast();
    //test_list_array();
    //test_struct_array();
    test_struct_array_build();
    //test_record_batch();
    //test_add_function();
    //test_substr_function();
    //test_split_function();
}