use consumer::TextConsumer;
use producer::TextProducer;
use texts::Texts;

mod consumer;
mod producer;
mod texts;

fn main() {
    let hosts = vec!["localhost:9092".to_owned()];
    let topic = "actions".to_string();

    let mut texts = Texts::new();
    let mut consumer = TextConsumer::new(hosts.clone(), topic);
    let mut producer = TextProducer::new(hosts);

    loop {
        for ms in consumer.consume_events().iter() {
            for m in ms.messages() {
                let event_data = TextConsumer::get_event_data(m);
                let action = event_data["action"].to_string();

                match action.as_str() {
                    "\"add\"" => {
                        let data = event_data["value"].to_string().replace("\"", "");
                        texts.add_text(data);
                    }
                    "\"remove\"" => {
                        let index = event_data["value"].to_string().parse::<usize>().unwrap();
                        texts.remove_text(index);
                    }
                    _ => println!("Invalid action")
                }

                producer.send_data_to_topic("texts", texts.to_json());
            }
            consumer.consume_messageset(ms);
        }
        consumer.commit_consumed();
    }
}
