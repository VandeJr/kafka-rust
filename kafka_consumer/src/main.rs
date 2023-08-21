use kafka::consumer::{Consumer, FetchOffset};
use std::str;

fn main() {
    let hosts = vec!["localhost:9092".to_owned()];
    let topic = "topic-name".to_owned();

    let mut consumer = Consumer::from_hosts(hosts)
        .with_topic(topic)
        .with_fallback_offset(FetchOffset::Latest)
        .create()
        .expect("Error creating consumer");

    loop {
        for ms in consumer.poll().unwrap().iter() {
            for m in ms.messages() {
                println!("{:?}", str::from_utf8(m.value).unwrap());
            }

            consumer.consume_messageset(ms).unwrap();
        }

        consumer.commit_consumed().unwrap();
    }
}
