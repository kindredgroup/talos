## Why do we need back pressure?

- When the rate of the incoming traffic is much higher than what the system can comfortably handle, this could lead to problems like serious degradation of performance, memory issues etc.
- In the case of replicator, this lead to the ***suffix*** growing too big quickly which leads to the service exhausting it’s memory and thereby the pod crashing.

## Design challenges.

- We cannot completely stop receiving messages from the certification topic, unless we know the ***suffix*** is not shrinking at all. This is because the single partition certification topic has both the `candidate` and `decision` messages.
    - `candidate` messages are used to write into the suffix.
    - `decision` messages are required to act on the message.
    - Therefore, we don’t want to completely stop the receiving messages, unless that is the worst case scenario. We need the `decision` to come in, for `statemaps` to be picked so that they can be installed.
    - And when we receive feedback of the `statemaps` installed, the `snapshot` can be updated in the db.
    - Updating the `snapshot` in turn is used to ***prune*** the *suffix* and ***commit** **offsets**.*
- Services in the Talos ecosystem are designed to be high throughput, low latency systems.
    - Very *eager* or *aggressive* backpressure can introduce high latencies and reducing the overall throughput.
    - At the same time, being too *passive* with the backpressure, can also lead to inverse effect on performance and high risk of memory issues.

## Scenarios for triggering backpressure in replicator:

1. When there are lot of certification requests from cohorts, but certifier is down.
2. When no `statemaps` are being installed.
3. When messages in certification topic are consumed at a much higher rate than the replicator can process.
4. When replicator was down for some period and comes back up, and there is a huge back log of messages in `certification` topic to process.

### Deeper understanding of various scenarios triggering backpressure in replicator

- **SCENARIO 1 -** When there are lot of certification requests from cohorts, but certifier is down.
    - This means there is a possibility of the topic being flooded by certification requests without decisions.
        - Say, certifier was down for 1 hour, and the cohorts were sending certification requests at a rate of 200 tps. That means, there is an accumulation of 720K candidates in the certifier topic without decisions.
    - This scenario cannot be handled by the backpressure logic in replicator. We need to get `decisions` to install statemaps. And under this scenario the probability of replicator going down before it gets to a `decision` is very high.
    - The longer the certifier is down and more `candidates` messages in the certification topic, the higher the risk of ***messenger*** or ***replicator*** running into OOM issue.
    - There was a discussion with architects, that the we should prevent spamming the certification topic, when decisions are not coming in. And this needs to be handled at the ***cohort initiator*** end.
        - Further discussion required with architects, before a solution for this is designed/worked.
- SCENARIO 2 - When no `statemaps` are being installed.
    - There could be few reasons, but the most prominent would be, the database is down and therefore installs are not happening.
    - When we detect such a state, where we have accumulated a lot of `statemaps` to install, but nothing is being installed. We have to do a hard stop, and not consume any more messages till some installs are happening and the suffix is shrinking.
- SCENARIO 3 - When messages in certification topic are consumed at a much higher rate than the replicator can process.
    - We are trying to bite more than what we can chew here.
    - Backpressure should kick in to slow the rate of incoming traffic, but dynamically keep adjusting so that we maintain a balance between applying the backpressure, but without having too much adverse effect on performance.
- SCENARIO 4 - When replicator was down for some period and comes back up, and there is a huge back log of messages in `certification` topic to process.
    - Think of a scenario where the pod crashed or a rebalance happened, during that period of time, if the *certification* topic has accumulated a lot of messages, as soon as the pod comes up, there will be a huge influx of incoming message, at a very high rate quickly filling up the suffix and immediately crashing the pod due to OOM.
    - Therefore in this scenario, backpressure must kick in to slow the rate as mentioned in ***SCENARIO 3***, with similar strategy.

## Suffix size comparison with and without backpressure

Starting replicator, when there are already 890K+ messages on the certifier topic.

Notice how in the replicator with backpressure, the suffix head doesn't grow as high as when running replicator without backpressure.

Also notice how this in turn makes the replicator faster with backpressure enabled in this scenario.


- Without backpressure
    - Time taken to process ~ 3 mins
    - Peak memory ~ 1.3Gb

![image.png](./suffix%20size%20for%20replicator%20without%20backpressure%20.png)

- With backpressure
    - Time taken to process ~1 min
    - Peak memory - 350 Mb

![image.png](./suffix%20size%20for%20replicator%20with%20backpressure.png)

## Core Design of backpressure implementation

- The core algorithm of the backpressure is
    - To calculate and return a ***timeout*** based on certain parameters.
    - And the service reacts based on the timeout computed.
- There are many strategies that can be used each with it’s own advantages and disadvantages.
    - Some thoughts were around using ***sliding window*** based approach and few other strategies.
    - But considering the tradeoffs and understanding of our talos based services, a mix of multiple strategies were blended to build the backpressure algorithm.
- We know that at the heart of `certifier`, `messenger` and `replicator` we have suffix. This is an in-memory custom data structure built using vecdequeue.
- Looking at the various scenarios mentioned in *Deeper understanding of various scenarios triggering backpressure in replicator*, we now understand the risk of not having backpressure and how it causes unstability of the service and crashes to the various services in talos eco-system that uses the suffix.
- The algorithm uses a mix of multiple strategies used for backpressure.
    - Rate-based(delta) - Reactive way of handling backpressure.
        - Look at the input (candidates) and output (suffix head) changing during a window.
            - In most of the talos services, the final outgoing task is done in some other thread, but one thing that is certain is, the suffix head will move only when we receive feedback from other thread, and we prune the suffix.
            - Therefore, tracking the movement of head, can indirectly help us interpret the outgoing rate.
    - Buffer threshold - Proactive way of handling backpressure.
        - When the suffix has crossed a threshold against a *configured* `max_suffix_size` , we compute the backpressure.
        - Since we are more sensitive to memory, this is prioritised over the previous rate based calculation.
    - Admission control
        - Calculating the timeout - The calculations from the above two steps are used to compute a timeout, which will be applied, before taking in the next input. This would allow sufficient time for the outgoing tasks to finish and the suffix to shrink.
        - Critical stop - If the outgoing tasks are not very slow or not progressing, and therefore the suffix head doesn’t change, we stop completely accepting any new incoming requests.
    - Decaying timeout (*Not part of the core, but expected to be used by the service implementing it)*
        - To minimise the frequency of computations for backpressure, it is computed after every `X` ms which is *configured* using `check_window_ms`
        - Between the checks, if there was a timeout, we use a *decay-weighted* timeout calculation, so that the timeout gradually reduces, this helps in controlling the latency better.

## Deeper look at the algorithm

![image.png](./Backpressure%20Core%20Logic.jpg)

## How it is used to control backpressure in Talos Replicator.

![image.png](./Replicator%20backpressure%20integration.jpg)