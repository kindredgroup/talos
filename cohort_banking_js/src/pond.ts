// A double-ended queue implemented with a growable ring buffer.
class ArrayDeque {
    private items: Array<any>
    private nextRead: number
    private size: number
    constructor(initialCapacity: number) {
      this.items = new Array(initialCapacity);
      this.nextRead = 0;
      this.size = 0;
    }

    addLast(item: any) {
      this.ensureCapacity(this.size + 1);
      this.items[(this.nextRead + this.size) % this.items.length] = item;
      this.size += 1;
    }

    ensureCapacity(requiredCapacity: number) {
      if (this.items.length < requiredCapacity) {
        //console.log("current length is " + this.items.length + ", growing!");
        let newItems = new Array(requiredCapacity * 2);
        for (let i = 0; i < this.size; i++) {
          newItems[i] = this.items[(this.nextRead + i) % this.items.length];
        }
        this.nextRead = 0;
        this.items = newItems;
      }
    }

    removeFirst(): any | undefined {
      if (this.size == 0) {
        return undefined;
      } else {
        let nextItem = this.items[this.nextRead];
        this.nextRead = (this.nextRead + 1) % this.items.length;
        this.size -= 1;
        return nextItem;
      }
    }
  }

  class WaitQueue {
    private items: ArrayDeque
    private waiters: ArrayDeque

    // The `initialCapacity` sets the initial size of the items queue.
    // The `readFactor` is an estimate of the number of concurrent waits (on item removal).
    constructor(private initialCapacity: number, private waitFactor: number) {
      this.items = new ArrayDeque(initialCapacity);
      this.waiters = new ArrayDeque(waitFactor);
    }

    // Adds an item to the tail of the queue.
    addLast(item: any) {
      let waiter = this.waiters.removeFirst();
      if (waiter === undefined) {
        this.items.addLast(item);
      } else {
        waiter(item);
      }
    }

    // Removes an item from the head of the queue, returning a promise that is fulfilled when an item becomes available.
    removeFirst(): Promise<any> {
      let item = this.items.removeFirst();
      if (item === undefined) {
        let waiters = this.waiters;
        let promise = new Promise(resolve => {
          waiters.addLast(resolve)
        });
        return promise;
      } else {
        return Promise.resolve(item);
      }
    }
  }

  const POISON = "poison";

  export class Pond {

    private queue: WaitQueue
    private handles: Array<Promise<void>>

    constructor(private concurrencyFactor: number) {
      this.queue = new WaitQueue(concurrencyFactor * 16, concurrencyFactor);
      this.handles = new Array();
      for (let i = 0; i < concurrencyFactor; i++) {
        var handle = new Promise<void>(async resolve => {
          while (true) {
            var task = await this.queue.removeFirst();
            if (task === POISON) {
              break;
            } else {
              await task();
            }
          }
          resolve();
        });
        this.handles.push(handle);
      }
    }

    // Submits a new async task for execution. The task is a parameterless function returning a promise.
    submit(task: any) {
      this.queue.addLast(task);
    }

    // Drains the underlying workers by dispensing a set of poison pills, returning a promise fulfilled when all workers terminate.
    shutdown(): Promise<any> {
      for (let i = 0; i < this.concurrencyFactor; i++) {
        this.queue.addLast(POISON);
      }
      return Promise.all(this.handles);
    }
  }