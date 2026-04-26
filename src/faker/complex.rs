use std::collections::{HashMap, HashSet};
use arrow_array::ArrayRef;
use arrow_schema::{DataType, Field, Fields};
use rand::Rng;
use serde::{Deserialize, Serialize};
use crate::faker::{DataBuilder, Faker, FakerConfig};
use crate::faker::expr::{parse_expr, Expr};


#[derive(Debug, Serialize, Deserialize)]
struct EvalConfig {
    expression: String,
}

#[typetag::serde(name = "eval")]
impl FakerConfig for EvalConfig {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        let expr = parse_expr(self.expression.as_str())?;
        Ok(Box::new(EvalFaker{ expr }))
    }
}

#[derive(Debug, Serialize,Deserialize)]
pub struct FieldFakerConfig {
    pub name: String,
    #[serde(flatten)]
    pub config: Box<dyn FakerConfig>,
}


#[derive(Debug, Serialize, Deserialize)]
struct StructFakerConfig {
    fields: Vec<FieldFakerConfig>,
}

#[typetag::serde(name = "struct")]
impl FakerConfig for StructFakerConfig {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        if self.fields.iter().map(|f| f.name.as_str()).collect::<HashSet<_>>().len() != self.fields.len() {
            return Err(anyhow::anyhow!("struct faker fields name must be unique"));
        }
        
        let mut field_fakers = Vec::with_capacity(self.fields.len());
        let mut fields = Vec::with_capacity(self.fields.len());

        for field_config in &self.fields {
            let faker = field_config.config.build()?;
            if faker.is_compute_faker() {
                return Err(anyhow::anyhow!("struct faker not support computed field: {}", field_config.name));
            }
            if faker.should_flatten_fields() {
                return Err(anyhow::anyhow!("struct faker not support flatten field: {}", field_config.name));
            }
            fields.push(Field::new(field_config.name.clone(), faker.data_type(), true));
            field_fakers.push(faker);
        }

        let faker = StructFaker::new(Fields::from(fields), field_fakers)?;
        Ok(Box::new(faker))
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct UnionFieldsConfig {
    fields: Vec<FieldFakerConfig>,
    weight: u32,
}

#[derive(Debug, Serialize, Deserialize)]
struct UnionFakerConfig {
    union_fields: Vec<UnionFieldsConfig>,
    #[serde(default = "default_random")]
    random: bool,
}

#[typetag::serde(name = "union")]
impl FakerConfig for UnionFakerConfig {
    fn build(&self) -> anyhow::Result<Box<dyn Faker>> {
        let mut fields_fakers: Vec<_> = Vec::with_capacity(self.union_fields.len());
        let mut field_info = HashMap::new();
        let mut fields = Vec::new();
        for union_fields_config in &self.union_fields {
            let weight = union_fields_config.weight;
            let field_configs = &union_fields_config.fields;
            if field_configs.iter().map(|f| f.name.as_str()).collect::<HashSet<_>>().len() != field_configs.len() {
                return Err(anyhow::anyhow!("union faker fields name must be unique"));
            }
            let mut field_fakers = HashMap::new();
            for field_config in field_configs {
                let faker = field_config.config.build()?;
                let field_len = field_info.len();
                if let Some((i, t)) = field_info.get(&field_config.name) {
                    let data_type = faker.data_type();
                    if data_type != * t {
                        return Err(anyhow::anyhow!(format!("Field {} has diff type {} and {}", field_config.name, data_type, t)));
                    }
                    field_fakers.insert(*i, faker);
                } else {
                    field_info.insert(&field_config.name, (field_len, faker.data_type()));
                    fields.push(Field::new(field_config.name.clone(), faker.data_type(), true));
                    field_fakers.insert(field_len, faker);
                }
            }

            fields_fakers.push((field_fakers, weight))
        }

        let fields_fakers = fields_fakers.into_iter().map(|x| {
            let (fakers, weight) = x;
            let mut field_fakers = Vec::with_capacity(fields.len());
            for i in 0..fields.len() {
                field_fakers.push((i, None))
            }
            for (i, faker) in fakers {
                field_fakers[i] = (i, Some(faker));
            }
            FieldsFaker::new(field_fakers, weight)
        }).collect();

        let faker = UnionFaker::new(Fields::from(fields), fields_fakers, self.random)?;
        Ok(Box::new(faker))
    }
}

fn default_random() -> bool {
    true
}

#[derive(Debug)]
pub struct ArrayFaker {
    ele_faker: Box<dyn Faker>,
    array_len_min: i32,
    array_len_max: i32,
}

impl ArrayFaker {
    pub fn new(ele_faker: Box<dyn Faker>, array_len_min: i32, array_len_max: i32) -> Self {
        ArrayFaker {
            ele_faker,
            array_len_min,
            array_len_max,
        }
    }
}

impl Faker for ArrayFaker {
    fn data_type(&self) -> DataType {
        DataType::new_list(self.ele_faker.data_type(), true)
    }

    fn init(&mut self) -> anyhow::Result<()> {
        self.ele_faker.init()?;
        Ok(())
    }

    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        let len = rand::rng().random_range(self.array_len_min..=self.array_len_max);
        match builder {
            DataBuilder::List{item_type, item_builder, offsets_builder, null_buffer_builder} => {
                for _ in 0..len {
                    self.ele_faker.gene_value(item_builder)?;
                }
                offsets_builder.push(item_builder.len() as i32);
                null_buffer_builder.append(true);
                Ok(())
            },
            _ => Err(anyhow::anyhow!("unexpected builder type"))
        }
    }
}

#[derive(Debug)]
pub struct StructFaker {
    fields: Fields,
    field_fakers: Vec<Box<dyn Faker>>,
}

impl StructFaker {
    pub fn new(fields: Fields, field_fakers: Vec<Box<dyn Faker>>) -> anyhow::Result<Self> {
        if fields.len() != field_fakers.len() {
            return Err(anyhow::anyhow!("fields and field_fakers length mismatch"));
        }
        
        // Validate that field types match faker data types
        for (field, faker) in fields.iter().zip(field_fakers.iter()) {
            if field.data_type() != &faker.data_type() {
                return Err(anyhow::anyhow!(
                    "field {} type mismatch: expected {:?}, got {:?}",
                    field.name(),
                    field.data_type(),
                    faker.data_type()
                ));
            }
        }
        
        Ok(StructFaker {
            fields,
            field_fakers,
        })
    }
}

impl Faker for StructFaker {
    fn data_type(&self) -> DataType {
        DataType::Struct(self.fields.clone())
    }

    fn init(&mut self) -> anyhow::Result<()> {
        for faker in self.field_fakers.iter_mut() {
            faker.init()?;
        }
        Ok(())
    }

    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        match builder {
            DataBuilder::Struct { fields: _, field_builders, null_buffer_builder } => {
                for (faker, field_builder) in self.field_fakers.iter_mut().zip(field_builders.iter_mut()) {
                    faker.gene_value(field_builder)?;
                }
                null_buffer_builder.append(true);
                Ok(())
            },
            _ => Err(anyhow::anyhow!("unexpected builder type, expected Struct builder"))
        }
    }
}

/// Wraps a child faker so each `gene_value` becomes a null with probability `null_rate`,
/// otherwise delegates to the child’s `gene_value`. `gene_null` always forwards to the child.
#[derive(Debug)]
pub struct NullAbleFaker {
    ele_faker: Box<dyn Faker>,
    null_rate: f32,
}

impl NullAbleFaker {
    pub fn new(ele_faker: Box<dyn Faker>, null_rate: f32) -> Self {
        Self {
            ele_faker,
            null_rate: if null_rate.is_finite() {
                null_rate.clamp(0.0, 1.0)
            } else {
                0.0
            },
        }
    }
}

impl Faker for NullAbleFaker {
    fn data_type(&self) -> DataType {
        self.ele_faker.data_type()
    }

    fn init(&mut self) -> anyhow::Result<()> {
        self.ele_faker.init()
    }

    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        if rand::rng().random_range(0.0f32..1.0f32) < self.null_rate {
            builder.append_null();
            Ok(())
        } else {
            self.ele_faker.gene_value(builder)
        }
    }
}

#[derive(Debug)]
pub struct EvalFaker {
    expr: Expr,
}

impl Faker for EvalFaker {
    fn data_type(&self) -> DataType {
        DataType::Null
    }

    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        Err(anyhow::anyhow!("compute faker not support gene value"))
    }

    fn is_compute_faker(&self) -> bool {
        true
    }

    fn compute(&self, columns: & HashMap<&str, ArrayRef>, row_count: usize) -> anyhow::Result<ArrayRef> {
        self.expr.eval(columns, row_count)
    }

}

#[derive(Debug)]
pub struct FieldsFaker {
    field_fakers: Vec<(usize, Option<Box<dyn Faker>>)>,
    weight: u32,
    weight_index: u32,
}

impl FieldsFaker {
    pub fn new(field_fakers: Vec<(usize, Option<Box<dyn Faker>>)>, weight: u32) -> Self {
        FieldsFaker {
            field_fakers,
            weight,
            weight_index: 0,
        }
    }
}

#[derive(Debug)]
pub struct UnionFaker {
    fields: Fields,
    fields_fakers: Vec<FieldsFaker>,
    random: bool,
    weights: Vec<u32>,
    weight_max: u32,
    index: usize,
}

impl UnionFaker {
    pub fn new(fields: Fields, fields_fakers: Vec<FieldsFaker>, random: bool) -> anyhow::Result<Self> {
        let tps:Vec<_> = fields.iter().map(|f| f.data_type()).collect();
        for FieldsFaker{field_fakers,  .. } in &fields_fakers {
            for (i, p) in field_fakers {
                if *i >= tps.len() {
                    return Err(anyhow::anyhow!("field index out of range"));
                }
                if let Some(faker) = p {
                    if *tps[*i] != faker.data_type() {
                        return Err(anyhow::anyhow!("field type mismatch"));
                    }
                    if faker.is_compute_faker() {
                        return Err(anyhow::anyhow!("union faker not support computed field"));
                    }
                }
            }
        }

        let mut weights = Vec::with_capacity(fields_fakers.len());
        for (i, faker) in fields_fakers.iter().enumerate() {
            if i == 0 {
                weights.push(faker.weight);
            } else {
                weights.push(faker.weight + weights[i - 1]);
            }
        }
        let weight_max = weights[weights.len() - 1];
        Ok(UnionFaker {
            fields,
            fields_fakers,
            random,
            weights,
            weight_max,
            index: 0,
        })
    }
}

impl Faker for UnionFaker {
    fn data_type(&self) -> DataType {
        DataType::Struct(self.fields.clone())
    }

    fn init(&mut self) -> anyhow::Result<()> {
        for fields_faker in self.fields_fakers.iter_mut() {
            for (_, field_faker) in fields_faker.field_fakers.iter_mut() {
                if let Some(faker) = field_faker {
                    faker.init()?;
                }
            }
        }
        Ok(())
    }

    fn gene_value(&mut self, builder: &mut DataBuilder) -> anyhow::Result<()> {
        match builder {
            DataBuilder::Struct { fields: _, field_builders, null_buffer_builder: _, } => {
                let mut fields_faker;
                if self.random {
                    let key = rand::rng().random_range(1..= self.weight_max) ;
                    let index = self.weights.binary_search(&key).unwrap_or_else(|i| i);
                    //println!("key: {}, index: {}, values: {:?}",  key, index,self.weights);
                    fields_faker = &mut self.fields_fakers[index];
                } else {
                    fields_faker = &mut self.fields_fakers[self.index];
                    if fields_faker.weight_index == fields_faker.weight {
                        fields_faker.weight_index = 0;
                        self.index += 1;
                        if self.index == self.fields_fakers.len() {
                            self.index = 0;
                        }
                        fields_faker = &mut self.fields_fakers[self.index];
                    }
                    fields_faker.weight_index += 1;
                }

                for (i, field_faker) in fields_faker.field_fakers.iter_mut() {
                    if let Some(faker) = field_faker {
                        faker.gene_value(&mut field_builders[*i])?;
                    } else {
                        field_builders[*i].append_null();
                    }
                }
                Ok(())
            },
            _ => Err(anyhow::anyhow!("unexpected builder type"))
        }
    }

    fn should_flatten_fields(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::Array;
    use arrow_array::cast::AsArray;
    use arrow_schema::Field;

    use crate::faker::{CharsStringFaker, OptionIntFaker, RangeFloatFaker, RangeIntFaker};
    use super::*;

    #[test]
    fn test_array_faker() {
        let mut faker = ArrayFaker::new(Box::new(OptionIntFaker::new(vec![Some(1), Some(2), Some(3), None], true).unwrap()), 1, 5);
        let mut builder = faker.data_builder(10).unwrap();
        for _ in 0..10 {
            faker.gene_value(&mut builder).unwrap();
        }
        let array = builder.finish().unwrap();
        println!("{}", array.len());
        println!("{:?}", array);
    }

    #[test]
    fn null_able_all_nulls_when_rate_one() {
        let mut faker = NullAbleFaker::new(
            Box::new(RangeIntFaker::new(1, 100, false).unwrap()),
            1.0,
        );
        let mut builder = faker.data_builder(20).unwrap();
        for _ in 0..20 {
            faker.gene_value(&mut builder).unwrap();
        }
        let array = builder.finish().unwrap();
        assert_eq!(array.len(), 20);
        assert_eq!(array.null_count(), 20);
    }

    #[test]
    fn null_able_no_random_null_when_rate_zero() {
        let mut faker = NullAbleFaker::new(
            Box::new(RangeIntFaker::new(7, 8, false).unwrap()),
            0.0,
        );
        let mut builder = faker.data_builder(5).unwrap();
        for _ in 0..5 {
            faker.gene_value(&mut builder).unwrap();
        }
        let array = builder.finish().unwrap();
        assert_eq!(array.null_count(), 0);
        assert!(array
            .as_primitive::<arrow_array::types::Int32Type>()
            .values()
            .iter()
            .all(|&v| v == 7));
    }

    #[test]
    fn null_able_gene_null_delegates() {
        let mut faker = NullAbleFaker::new(
            Box::new(RangeIntFaker::new(1, 10, true).unwrap()),
            0.5,
        );
        let mut builder = faker.data_builder(10).unwrap();
        for _ in 0..10 {
            faker.gene_value(&mut builder).unwrap();
        }
        let array = builder.finish().unwrap();
        assert_eq!(array.len(), 10);
        println!("{:?}", array);
    }

    #[test]
    fn test_union_faker_random() {
        // 创建字段
        let fields = Fields::from(vec![
            Field::new("id", arrow_schema::DataType::Int32, true),
            Field::new("name", arrow_schema::DataType::Utf8, true),
        ]);

        // 创建FieldsFaker
        let fields_faker1 = FieldsFaker::new(
            vec![
                (0, Some(Box::new(RangeIntFaker::new(1, 10, false).unwrap()))),
                (1, Some(Box::new(CharsStringFaker::new(vec!['a', 'b', 'c'], 5).unwrap()))),
            ],
            1,
        );

        let fields_faker2 = FieldsFaker::new(
            vec![
                (0, Some(Box::new(RangeIntFaker::new(100, 200, false).unwrap()))),
                (1, Some(Box::new(CharsStringFaker::new(vec!['1', '2', '3'], 5).unwrap()))),
            ],
            2,
        );

        // 创建UnionFaker（随机模式）
        let mut union_faker = UnionFaker::new(fields, vec![fields_faker1, fields_faker2], true).unwrap();
        let mut builder = union_faker.data_builder(10).unwrap();

        // 生成数据
        for _ in 0..30 {
            union_faker.gene_value(&mut builder).unwrap();
        }

        let array = builder.finish().unwrap();
        assert_eq!(array.len(), 30);
        println!("UnionFaker random mode result: {:?}", array);
    }

    #[test]
    fn test_struct_faker() {
        // 创建StructFaker
        let fields = Fields::from(vec![
            Field::new("id", DataType::Int32, true),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Float32, true),
        ]);
        
        let field_fakers: Vec<Box<dyn Faker>> = vec![
            Box::new(RangeIntFaker::new(1, 100, false).unwrap()),
            Box::new(CharsStringFaker::new(vec!['a', 'b', 'c'], 5).unwrap()),
            Box::new(RangeFloatFaker::new(0.0, 100.0).unwrap()),
        ];
        
        let mut struct_faker = StructFaker::new(fields, field_fakers).unwrap();
        let mut builder = struct_faker.data_builder(10).unwrap();
        
        // 生成数据
        for _ in 0..10 {
            struct_faker.gene_value(&mut builder).unwrap();
        }
        
        let array = builder.finish().unwrap();
        assert_eq!(array.len(), 10);
        
        // 验证是StructArray
        let struct_array = array.as_any().downcast_ref::<arrow_array::StructArray>().unwrap();
        assert_eq!(struct_array.fields().len(), 3);
        
        println!("StructFaker result: {:?}", array);
    }

    #[test]
    fn test_struct_faker_config() {
        // 通过JSON配置创建StructFaker
        let json = r#"{
            "type": "struct",
            "fields": [
                {"name": "id", "type": "int", "min": 1, "max": 100, "random": false},
                {"name": "name", "type": "string", "options": ["Alice", "Bob", "Charlie"]},
                {"name": "age", "type": "int", "min": 18, "max": 60}
            ]
        }"#;
        
        let config: StructFakerConfig = serde_json::from_str(json).unwrap();
        let mut faker = config.build().unwrap();
        let mut builder = faker.data_builder(5).unwrap();
        
        for _ in 0..5 {
            faker.gene_value(&mut builder).unwrap();
        }
        
        let array = builder.finish().unwrap();
        assert_eq!(array.len(), 5);
        
        let struct_array = array.as_any().downcast_ref::<arrow_array::StructArray>().unwrap();
        assert_eq!(struct_array.fields().len(), 3);
        
        println!("StructFaker from config: {:?}", array);
    }

}