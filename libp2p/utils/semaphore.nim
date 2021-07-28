## Nim-Libp2p
## Copyright (c) 2020 Status Research & Development GmbH
## Licensed under either of
##  * Apache License, version 2.0, ([LICENSE-APACHE](LICENSE-APACHE))
##  * MIT license ([LICENSE-MIT](LICENSE-MIT))
## at your option.
## This file may not be copied, modified, or distributed except according to
## those terms.

{.push raises: [Defect].}

import sequtils
import chronos, chronicles

# TODO: this should probably go in chronos

logScope:
  topics = "libp2p semaphore"

type
  AsyncSemaphore* = ref object of RootObj
    size: int
    count: int
    queue: seq[Future[void]]

proc newAsyncSemaphore*(size: int): AsyncSemaphore =
  AsyncSemaphore(size: size, count: 0)

proc `count`*(s: AsyncSemaphore): int = s.count

proc `size`*(s: AsyncSemaphore): int = s.size

proc setSize*(s: AsyncSemaphore, size: int) =
  if s.size == size: return

  if s.size < size:
    while s.count < size and s.queue.len > 0:
      let fut = s.queue[0]
      s.queue.delete(0)
      if not fut.finished():
        s.count.inc
        fut.complete()

  s.size = size

proc tryAcquire*(s: AsyncSemaphore): bool =
  ## Attempts to acquire a resource, if successful
  ## returns true, otherwise false
  ##

  if s.count < s.size and s.queue.len == 0:
    s.count.inc
    trace "Acquired slot", available = s.count, queue = s.queue.len
    return true

  return false

proc acquire*(s: AsyncSemaphore): Future[void] =
  ## Acquire a resource and decrement the resource
  ## counter. If no more resources are available,
  ## the returned future will not complete until
  ## the resource count goes above 0.
  ##

  let fut = newFuture[void]("AsyncSemaphore.acquire")
  if s.tryAcquire():
    fut.complete()
    return fut

  proc cancellation(udata: pointer) {.gcsafe.} =
    fut.cancelCallback = nil
    if not fut.finished:
      s.queue.keepItIf( it != fut )

  fut.cancelCallback = cancellation

  s.queue.add(fut)

  trace "Queued slot", available = s.count, queue = s.queue.len
  return fut

proc release*(s: AsyncSemaphore) =
  ## Release a resource from the semaphore,
  ## by picking the first future from the queue
  ## and completing it and incrementing the
  ## internal resource count
  ##

  if s.count > 0:
    trace "Releasing slot", available = s.count,
                            queue = s.queue.len

    s.count.dec # increment the resource count

    if s.queue.len > 0 and s.count < s.size:
      var fut = s.queue[0]
      s.queue.delete(0)
      if not fut.finished():
        s.count.inc
        fut.complete()

    trace "Released slot", available = s.count,
                           queue = s.queue.len
