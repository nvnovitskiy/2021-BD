# L4 - ZooKeeper

____

## Цель работы

+ запустить ZooKeeper,
+ изучить директорию с установкой ZooKeeper,
+ запустить интерактивную сессию ZooKeeper CLI и освоить её команды,
+ научиться проводить мониторинг ZooKeeper,
+ разработать приложение с барьерной синхронизацией, основанной на ZooKeeper,
+ запустить и проверить работу приложения.

## Выполненные задания

____

+ Animal

```scala
// Animal.scala

package zoo

import org.apache.zookeeper._

case class Animal( name: String, hostPort: String, root: String, partySize: Integer)
  extends Watcher
{
  val zk = new ZooKeeper(hostPort, 1000, this)
  val mutex = new Object()
  val animalPath: String = root + "/" + name

  if (zk == null) throw new Exception("ZK is NULL.")

  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      println(s"Event from keeper: ${event.getType}")
      mutex.notify()
    }
  }

  def enter(): Boolean = {
    zk.create(animalPath, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
    mutex.synchronized {
      while (true) {
        val party = zk.getChildren(root, this)
        if (party.size() < partySize) {
          println("Waiting for the others.")
          mutex.wait()
          println("Noticed someone.")
        } else {
          return true
        }
      }
    }
    false
  }

  def leave(): Unit = {
    zk.delete(animalPath, -1)
  }
}

// Main.scala

package zoo
import scala.util.Random

object Main {
  val sleepTime = 3000

  def main(args: Array[String]): Unit = {
    println("Starting animal runner")

    val Seq(animalName, hostPort, partySize) = args.toSeq

    val animal = Animal(animalName, hostPort, "/zoo", partySize.toInt)

    try {
      animal.enter()
      println(s"${animal.name} entered.")
      for (i <- 1 to Random.nextInt(20)) {
        Thread.sleep(sleepTime)
        println(s"${animal.name} is running...")
      }
      animal.leave()

    } catch {
      case e: Exception => println("Animal was not permitted to the zoo." + e)
    }
  }
}

Starting animal runner
log4j:WARN No appenders could be found for logger (org.apache.zookeeper.ZooKeeper).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
Event from keeper: None
Waiting for the others.

Starting animal runner
log4j:WARN No appenders could be found for logger (org.apache.zookeeper.ZooKeeper).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
Event from keeper: None
tiger entered.
tiger is running...
tiger is running...
tiger is running...
tiger is running...
Event from keeper: NodeChildrenChanged
```

+ Philosopher

```scala
// Philosopher.scala

package Philosophers

import java.util.concurrent.Semaphore
import org.apache.zookeeper._
import scala.util.Random

case class Philosopher(id: Int,
                       hostPort: String,
                       root: String,
                       left: Semaphore,
                       right: Semaphore,
                       seats: Integer) extends Watcher {
  val zk = new ZooKeeper(hostPort, 3000, this)
  val mutex = new Object()
  val path: String = root + "/" + id.toString

  if (zk == null) throw new Exception("ZK is NULL.")

  override def process(event: WatchedEvent): Unit = {
    mutex.synchronized {
      mutex.notify()
    }
  }

  def eat(): Boolean = {
    printf("Philosopher %d is going to eat\n", id)
    mutex.synchronized {
      var created = false
      while (true) {
        if (!created) {
          zk.create(path, Array.emptyByteArray, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL)
          created = true
        }
        val active = zk.getChildren(root, this)
        if (active.size() > seats) {
          zk.delete(path, -1)
          mutex.wait(3000)
          Thread.sleep(Random.nextInt(5)* 100)
          created = false
        } else {
          left.acquire()
          printf("Philosopher %d picked up the left fork\n", id)
          right.acquire()
          printf("Philosopher %d picked up the right fork\n", id)
          Thread.sleep((Random.nextInt(5) + 1) * 1000)
          right.release()
          printf("Philosopher %d put the right fork\n", id)
          left.release()
          printf("Philosopher %d put the loft fork and finished eating\n", id)
          return true
        }
      }
    }
    false
  }

  def think(): Unit = {
    printf("Philosopher %d is thinking\n", id)
    zk.delete(path, -1)
    Thread.sleep((Random.nextInt(5) + 1) * 1000)
  }
}

// Main_philosopher.scala

package Philosophers

import java.util.concurrent.Semaphore

object Main_philosophers {
  def main(args: Array[String]): Unit = {
    val hostPort = "localhost:2181"
    val philosophersCount = 3
    val seats = philosophersCount - 1

    val forks = new Array[Semaphore](philosophersCount)
    for (j <- 0 until philosophersCount){
      forks(j) = new Semaphore(1)
    }

    val threads = new Array[Thread](philosophersCount)
    for (id <- 0 until philosophersCount){
      threads(id) = new Thread(
        new Runnable {
          def run(): Unit = {
            val i = (id + 1) % philosophersCount
            val philosopher = Philosopher(id, hostPort, "/ph".toString, forks(id), forks(i), seats)
            for (j <- 1 to 2) {
              philosopher.eat()
              philosopher.think()
            }
          }
        }
      )
      threads(id).setDaemon(false)
      threads(id).start()
    }
    for (id <- 0 until philosophersCount){
      threads(id).join()
    }
  }
}

log4j:WARN No appenders could be found for logger (org.apache.zookeeper.ZooKeeper).
log4j:WARN Please initialize the log4j system properly.
log4j:WARN See http://logging.apache.org/log4j/1.2/faq.html#noconfig for more info.
Philosopher 0 is going to eat
Philosopher 1 is going to eat
Philosopher 2 is going to eat
Philosopher 2 picked up the left fork
Philosopher 0 picked up the left fork
Philosopher 0 picked up the right fork
Philosopher 0 put the right fork
Philosopher 0 put the loft fork and finished eating
Philosopher 2 picked up the right fork
Philosopher 0 is thinking
Philosopher 1 picked up the left fork
Philosopher 0 is going to eat
Philosopher 2 put the right fork
Philosopher 2 put the loft fork and finished eating
Philosopher 2 is thinking
Philosopher 1 picked up the right fork
Philosopher 0 picked up the left fork
Philosopher 2 is going to eat
Philosopher 1 put the right fork
Philosopher 1 put the loft fork and finished eating
Philosopher 0 picked up the right fork
Philosopher 1 is thinking
Philosopher 2 picked up the left fork
Philosopher 0 put the right fork
Philosopher 0 put the loft fork and finished eating
Philosopher 2 picked up the right fork
Philosopher 0 is thinking
Philosopher 1 is going to eat
Philosopher 2 put the right fork
Philosopher 2 put the loft fork and finished eating
Philosopher 2 is thinking
Philosopher 1 picked up the left fork
Philosopher 1 picked up the right fork
Philosopher 1 put the right fork
Philosopher 1 put the loft fork and finished eating
Philosopher 1 is thinking

Process finished with exit code 0
```

+ Two Phase Commit
```python
from logging import basicConfig
from threading import Thread
from time import sleep
from random import random
from kazoo.client import KazooClient

basicConfig()


class Client(Thread):
    def __init__(self, root: str, idx: int):
        super().__init__()
        self.url = f"{root}/{idx}"
        self.root = root
        self.id = idx

    def run(self):
        kc = KazooClient()
        kc.start()

        val = b"commit" if random() > 0.5 else b"abort"
        print(f"Client {self.id} request {val.decode()}")
        kc.create(self.url, val, ephemeral=True)
        
        @kc.DataWatch(self.url)
        def watch(data, stat):
            if stat.version > 0:
                print(f"Client {self.id} do {data.decode()}")

        sleep(5)
        kc.stop()
        kc.close()
        

class Coordinator():
    def main(self):
        coord = KazooClient()
        coord.start()

        if coord.exists("/coord"):
            coord.delete("/coord", recursive=True)

        coord.create("/coord")
        coord.create("/coord/crd")
        n_clients = 5

        def solution():
            clients = coord.get_children("/coord/crd")
            commits, aborts = 0, 0
            for clt in clients:
                commits += int(coord.get(f"/coord/crd/{clt}")[0] == b"commit")
                aborts += int(coord.get(f"/coord/crd/{clt}")[0] == b"abort")

            for clt in clients:
                coord.set(f"/coord/crd/{clt}", b"commit" if commits > aborts else b"abort")

        @coord.ChildrenWatch("/coord/crd")
        def check_clients(clients):
            if len(clients) < n_clients:
                print(f"Waiting others clients: {clients}")
            elif len(clients) == n_clients:
                print("Check clients")
                solution()

        for x in range(5):
            begin=Client("/coord/crd", x)
            begin.start()


if __name__ == "__main__":
    Coordinator().main()
    
Waiting others clients: []
Client 0 request commit
Client 1 request commit
Client 2 request abort
Client 4 request commit
Client 3 request abort
Waiting others clients: ['0', '1', '2', '4']
Check clients
Client 0 do commit
Client 1 do commit
Client 2 do commit
Client 3 do commit
Client 4 do commit
Waiting others clients: []
```
