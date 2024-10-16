import trio 
import time 

# Defining an asynchronous function
async def child1():
    print(" Child1: started! sleeping now... ")
    # When given to the even loop, this await function will identify as an asynchrnous operation
    await trio.sleep(1)
    print(" child: exiting!")

async def child2():
    print(" child2: started! sleeping now...")
    await trio.sleep(2)
    print(" child2: exiting")

async def parent():
    print("parent: started!")
    async with trio.open_nursery as nursery:
        print("Parent: Spawning child")
        nursery.start_soon(child1)

        print("parent: spawning child2...")
