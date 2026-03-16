use std::collections::HashMap;
use std::time::Duration;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use rdkafka::{ClientConfig, TopicPartitionList};
use rdkafka::consumer::{BaseConsumer, Consumer};

/// Register Kafka tools functions to the module
pub fn register(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(get_kafka_topic_info, module)?)?;
    module.add_function(wrap_pyfunction!(get_kafka_group_info, module)?)?;
    module.add_function(wrap_pyfunction!(reset_kafka_group_offset_latest, module)?)?;
    module.add_function(wrap_pyfunction!(reset_kafka_group_offset_for_ts, module)?)?;
    Ok(())
}

/// Get Kafka topic information including partitions and their offsets
///
/// Args:
///     brokers: Kafka bootstrap servers (e.g., "localhost:9092")
///     topic: Kafka topic name
///     properties: Optional dict of Kafka consumer config
///
/// Returns:
///     Dict with topic information, including partitions and their offsets
#[pyfunction]
#[pyo3(signature = (
    brokers,
    topic,
    properties=None
))]
pub fn get_kafka_topic_info<'py>(
    py: Python<'py>,
    brokers: &str,
    topic: &str,
    properties: Option<&Bound<'_, PyDict>>,
) -> PyResult<Bound<'py, PyDict>> {
    let props = properties.map(py_dict_to_hashmap).transpose()?;
    let props = props.unwrap_or(HashMap::new());

    let topic = topic.to_string();
    let brokers = brokers.to_string();
    let props = props;

    let result_data = py.detach(|| {
        get_kafka_topic_info_impl(&brokers, &topic, &props)
    })
    .map_err(PyRuntimeError::new_err)?;

    let (found, topic, partitions) = result_data;
    let result = PyDict::new(py);
    
    if found {
        let partitions_dict = PyDict::new(py);
        
        for (partition_id, (low, high)) in partitions {
            let partition_info = PyDict::new(py);
            partition_info.set_item("low_offset", low)?;
            partition_info.set_item("high_offset", high)?;
            partition_info.set_item("lag", high - low)?;
            
            partitions_dict.set_item(partition_id, partition_info)?;
        }
        
        result.set_item("topic", topic)?;
        result.set_item("partitions", partitions_dict)?;
    } else {
        result.set_item("error", format!("Topic '{}' not found", topic))?;
    }
    
    Ok(result)
}

fn get_kafka_topic_info_impl(
    brokers: &str,
    topic: &str,
    props: &HashMap<String, String>,
) -> Result<(bool, String, HashMap<i32, (i64, i64)>), String> {
    let consumer = create_consumer(brokers, None, props)
        .map_err(|e| format!("Failed to create consumer: {e}"))?;

    let metadata = consumer.fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|e| format!("Failed to fetch metadata: {e}"))?;
    
    let mut partitions = HashMap::new();
    let mut found = false;
    
    if let Some(t) = metadata.topics().iter().find(|t| t.name() == topic) {
        found = true;
        for partition in t.partitions() {
            let (low, high) = consumer.fetch_watermarks(topic, partition.id(), Duration::from_secs(5))
                .map_err(|e| format!("Failed to fetch watermarks: {e}"))?;
            
            partitions.insert(partition.id(), (low, high));
        }
    }
    
    Ok((found, topic.to_string(), partitions))
}

/// Get Kafka consumer group information including offsets and lag
///
/// Args:
///     brokers: Kafka bootstrap servers (e.g., "localhost:9092")
///     topic: Kafka topic name
///     group_id: Consumer group ID
///     properties: Optional dict of Kafka consumer config
///
/// Returns:
///     Dict with group information, including partitions, offsets, and lag
#[pyfunction]
#[pyo3(signature = (
    brokers,
    topic,
    group_id,
    properties=None
))]
pub fn get_kafka_group_info<'py>(
    py: Python<'py>,
    brokers: &str,
    topic: &str,
    group_id: &str,
    properties: Option<&Bound<'_, PyDict>>,
) -> PyResult<Bound<'py, PyDict>> {
    let props = properties.map(py_dict_to_hashmap).transpose()?;
    let props = props.unwrap_or(HashMap::new());

    let topic = topic.to_string();
    let brokers = brokers.to_string();
    let group_id = group_id.to_string();
    let props = props;

    let result_data = py.detach(|| {
        get_kafka_group_info_impl(&brokers, &topic, &group_id, &props)
    })
    .map_err(PyRuntimeError::new_err)?;

    let (found, topic, group_id, partitions, total_lag) = result_data;
    let result = PyDict::new(py);
    
    if found {
        let partitions_dict = PyDict::new(py);
        
        for (partition_id, (committed_offset, high_offset, lag)) in partitions {
            let partition_info = PyDict::new(py);
            partition_info.set_item("committed_offset", committed_offset)?;
            partition_info.set_item("high_offset", high_offset)?;
            partition_info.set_item("lag", lag)?;
            
            partitions_dict.set_item(partition_id, partition_info)?;
        }
        
        result.set_item("topic", topic)?;
        result.set_item("group_id", group_id)?;
        result.set_item("partitions", partitions_dict)?;
        result.set_item("total_lag", total_lag)?;
    } else {
        result.set_item("error", format!("Topic '{}' not found", topic))?;
    }
    
    Ok(result)
}

fn get_kafka_group_info_impl(
    brokers: &str,
    topic: &str,
    group_id: &str,
    props: &HashMap<String, String>,
) -> Result<(bool, String, String, HashMap<i32, (i64, i64, i64)>, i64), String> {
    let consumer = create_consumer(brokers, Some(group_id), props)
        .map_err(|e| format!("Failed to create consumer: {e}"))?;
    
    let metadata = consumer.fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|e| format!("Failed to fetch metadata: {e}"))?;
    
    let mut partitions = HashMap::new();
    let mut total_lag = 0;
    let mut found = false;
    
    if let Some(t) = metadata.topics().iter().find(|t| t.name() == topic) {
        found = true;
        let mut tpl = TopicPartitionList::new();
        let mut partition_offsets = HashMap::new();
        
        for partition in t.partitions() {
            let (_, high) = consumer.fetch_watermarks(topic, partition.id(), Duration::from_secs(5))
                .map_err(|e| format!("Failed to fetch watermarks: {e}"))?;
            partition_offsets.insert(partition.id(), high);
            tpl.add_partition(topic, partition.id());
        }
        
        let committed_offsets = consumer.committed_offsets(tpl, Duration::from_secs(10))
            .map_err(|e| format!("Failed to fetch committed offsets: {e}"))?;
        
        for elem in committed_offsets.elements() {
            let offset = match elem.offset() {
                rdkafka::Offset::Offset(offset) => offset,
                _ => 0,
            };
            
            let partition_offset = partition_offsets.get(&elem.partition()).unwrap();
            let lag = partition_offset - offset;
            total_lag += lag;
            
            partitions.insert(elem.partition(), (offset, *partition_offset, lag));
        }
    }
    
    Ok((found, topic.to_string(), group_id.to_string(), partitions, total_lag))
}

/// Reset Kafka consumer group offset to latest (high watermark)
///
/// Args:
///     brokers: Kafka bootstrap servers (e.g., "localhost:9092")
///     topic: Kafka topic name
///     group_id: Consumer group ID
///     properties: Optional dict of Kafka consumer config
///
/// Returns:
///     Dict with reset status
#[pyfunction]
#[pyo3(signature = (
    brokers,
    topic,
    group_id,
    properties=None
))]
pub fn reset_kafka_group_offset_latest<'py>(
    py: Python<'py>,
    brokers: &str,
    topic: &str,
    group_id: &str,
    properties: Option<&Bound<'_, PyDict>>,
) -> PyResult<Bound<'py, PyDict>> {
    let props = properties.map(py_dict_to_hashmap).transpose()?;
    let props = props.unwrap_or(HashMap::new());

    let topic = topic.to_string();
    let brokers = brokers.to_string();
    let group_id = group_id.to_string();
    let props = props;

    let result_data = py.detach(|| {
        reset_kafka_group_offset_latest_impl(&brokers, &topic, &group_id, &props)
    })
    .map_err(PyRuntimeError::new_err)?;

    let (found, topic, group_id, partitions) = result_data;
    let result = PyDict::new(py);
    
    if found {
        let partitions_dict = PyDict::new(py);
        
        for (partition_id, offset) in partitions {
            partitions_dict.set_item(partition_id, offset)?;
        }
        
        result.set_item("status", "success")?;
        result.set_item("topic", topic)?;
        result.set_item("group_id", group_id)?;
        result.set_item("reset_offsets", partitions_dict)?;
    } else {
        result.set_item("status", "error")?;
        result.set_item("error", format!("Topic '{}' not found", topic))?;
    }
    
    Ok(result)
}

fn reset_kafka_group_offset_latest_impl(
    brokers: &str,
    topic: &str,
    group_id: &str,
    props: &HashMap<String, String>,
) -> Result<(bool, String, String, HashMap<i32, i64>), String> {
    let consumer = create_consumer(brokers, Some(group_id), props)
        .map_err(|e| format!("Failed to create consumer: {e}"))?;

    let metadata = consumer.fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|e| format!("Failed to fetch metadata: {e}"))?;
    
    let mut partitions = HashMap::new();
    let mut found = false;
    
    if let Some(t) = metadata.topics().iter().find(|t| t.name() == topic) {
        found = true;
        let mut tpl = TopicPartitionList::new();
        
        for partition in t.partitions() {
            let (_, high) = consumer.fetch_watermarks(topic, partition.id(), Duration::from_secs(5))
                .map_err(|e| format!("Failed to fetch watermarks: {e}"))?;
            
            let offset = rdkafka::Offset::Offset(high);
            tpl.add_partition_offset(topic, partition.id(), offset)
                .map_err(|e| format!("Failed to add partition offset: {e}"))?;
            
            partitions.insert(partition.id(), high);
        }
        
        consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync)
            .map_err(|e| format!("Failed to commit offsets: {e}"))?;
    }
    
    Ok((found, topic.to_string(), group_id.to_string(), partitions))
}

/// Reset Kafka consumer group offset to specific timestamp
///
/// Args:
///     brokers: Kafka bootstrap servers (e.g., "localhost:9092")
///     topic: Kafka topic name
///     group_id: Consumer group ID
///     timestamp_ms: Timestamp in milliseconds
///     properties: Optional dict of Kafka consumer config
///
/// Returns:
///     Dict with reset status and offset information
#[pyfunction]
#[pyo3(signature = (
    brokers,
    topic,
    group_id,
    timestamp_ms,
    properties=None
))]
pub fn reset_kafka_group_offset_for_ts<'py>(
    py: Python<'py>,
    brokers: &str,
    topic: &str,
    group_id: &str,
    timestamp_ms: i64,
    properties: Option<&Bound<'_, PyDict>>,
) -> PyResult<Bound<'py, PyDict>> {
    let props = properties.map(py_dict_to_hashmap).transpose()?;
    let props = props.unwrap_or(HashMap::new());
    
    let topic = topic.to_string();
    let brokers = brokers.to_string();
    let group_id = group_id.to_string();
    let props = props;

    let result_data = py.detach(|| {
        reset_kafka_group_offset_for_ts_impl(&brokers, &topic, &group_id, timestamp_ms, &props)
    })
    .map_err(PyRuntimeError::new_err)?;

    let (topic, group_id, timestamp_ms, partitions) = result_data;
    let result = PyDict::new(py);
    let partitions_dict = PyDict::new(py);
    
    for (partition_id, offset) in partitions {
        partitions_dict.set_item(partition_id, offset)?;
    }
    
    result.set_item("status", "success")?;
    result.set_item("topic", topic)?;
    result.set_item("group_id", group_id)?;
    result.set_item("timestamp_ms", timestamp_ms)?;
    result.set_item("reset_offsets", partitions_dict)?;
    
    Ok(result)
}

fn reset_kafka_group_offset_for_ts_impl(
    brokers: &str,
    topic: &str,
    group_id: &str,
    timestamp_ms: i64,
    props: &HashMap<String, String>,
) -> Result<(String, String, i64, HashMap<i32, Option<i64>>), String> {
    let consumer = create_consumer(brokers, Some(group_id), props)
        .map_err(|e| format!("Failed to create consumer: {e}"))?;
    
    let metadata = consumer.fetch_metadata(Some(topic), Duration::from_secs(10))
        .map_err(|e| format!("Failed to fetch metadata: {e}"))?;
    
    let topic_metadata = metadata.topics()
        .get(0).ok_or_else(|| format!("No topic found"))?;
    
    let mut tpl = TopicPartitionList::new();
    for partition in topic_metadata.partitions() {
        tpl.add_partition_offset(topic, partition.id(), rdkafka::Offset::Offset(timestamp_ms))
            .map_err(|e| format!("Failed to add partition offset: {e}"))?;
    }
    
    let offsets = consumer.offsets_for_times(tpl, Duration::from_secs(10))
        .map_err(|e| format!("Failed to get offsets for times: {e}"))?;
    
    let mut tpl = TopicPartitionList::new();
    let mut partitions = HashMap::new();
    
    for elem in offsets.elements() {
        if let Some(offset) = elem.offset().to_raw() {
            tpl.add_partition_offset(topic, elem.partition(), rdkafka::Offset::Offset(offset))
                .map_err(|e| format!("Failed to add partition offset: {e}"))?;
            partitions.insert(elem.partition(), Some(offset));
        } else {
            partitions.insert(elem.partition(), None);
        }
    }
    
    consumer.commit(&tpl, rdkafka::consumer::CommitMode::Sync)
        .map_err(|e| format!("Failed to commit offsets: {e}"))?;
    
    Ok((topic.to_string(), group_id.to_string(), timestamp_ms, partitions))
}

fn create_consumer(brokers: &str, group_id: Option<&str>, props: &HashMap<String, String>) -> Result<BaseConsumer, rdkafka::error::KafkaError> {
    let mut config = ClientConfig::new();
    config.set("bootstrap.servers", brokers).set("enable.auto.commit", "false");
    
    if let Some(group) = group_id {
        config.set("group.id", group);
    }
    
    for (k, v) in props {
        config.set(k, v);
    }
    
    config.create()
}

fn py_dict_to_hashmap(dict: &Bound<'_, PyDict>) -> PyResult<HashMap<String, String>> {
    let mut out = HashMap::with_capacity(dict.len());
    for (k, v) in dict.iter() {
        let key = k.extract::<String>()?;
        let val = v.extract::<String>()?;
        out.insert(key, val);
    }
    Ok(out)
}
