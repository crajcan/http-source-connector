use std::sync::atomic::Ordering;
use std::thread::sleep;
use tide::sse::Sender;
use tide::Request;

use std::time::Duration;

use crate::State;

pub(crate) async fn stream_count_updates_sporadically(req: Request<State>, sender: Sender) -> tide::Result<()> {
    let mut get_count = 0;
    let mut post_count = 0;
    let state = req.state();

    loop {
        construct_and_send_request_counts(&state, &sender, &mut get_count, &mut post_count).await?;

        sleep(Duration::from_millis(100));
    }
}

async fn construct_and_send_request_counts(
    state: &State,
    sender: &Sender,
    get_count: &mut u32,
    post_count: &mut u32,
) -> tide::Result<()> {
    let new_get_count = &state.get_count.load(Ordering::Relaxed);
    let new_post_count = &state.post_count.load(Ordering::Relaxed);

    let get_received = *new_get_count > *get_count;
    let post_received = *new_post_count > *post_count;

    let event = match (get_received, post_received) {
        (true, true) => Some("get and post requests"),
        (true, false) => Some("get request(s)"),
        (false, true) => Some("post request(s)"),
        (false, false) => None,
    };

    if let Some(event_name) = event {
        *get_count = *new_get_count;
        *post_count = *new_post_count;

        let counts = format!(
            "{{ \"gets\": {}, \"posts\": {} }}",
            *new_get_count, *new_post_count
        );

        if *get_count % 3 == 0 && *get_count != 0  {
            return Err(tide::Error::from_str(500, "Internal Server Error")); 
        } else {
            sender.send(event_name, counts, None).await?;
        }
    }

    Ok(())
}