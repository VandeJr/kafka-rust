use kafka::producer::{Producer, Record};

fn main() {
    let hosts = vec!["localhost:9092".to_owned()];
    let topic = "topic-name".to_owned();

    let mut producer = Producer::from_hosts(hosts)
        .create().expect("Error creating producer");

    for i in 0..10 {
        let buf = format!("{i}");
        producer.send(&Record::from_value(&topic, buf.as_bytes()))
            .unwrap();
        println!("Sent: {i}");
    }
}
