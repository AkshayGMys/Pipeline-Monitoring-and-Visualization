""" import asyncio

i = 1
class t:
    async def st(self):
        try:
            raise Exception("Sayonara Zankoku no Sekai")
        except Exception as e:
            print(e)
            return 'None'
        
    async def func1(self, topic = "Hekki af"):
        global i
        await asyncio.sleep(1)
        ans = await asyncio.gather(self.st())
        i+=1
        return topic+ans[0] 

    async def func2(self,topic = "adhf af"):
        await asyncio.sleep(1)
        return topic

    async def func3(self,topic = "bvnvc af"):
        await asyncio.sleep(1)
       
        return topic
   
def main():
    s = t()
    l = ["Hello", "Wordl","WHaa are oy","fahl","afsd"]
    loop = asyncio.get_event_loop()
    tasks = *(s.func1(t) for t in l),*(s.func2(m) for m in l),*(s.func3(n) for n in l)

    res = loop.run_until_complete(asyncio.gather(*tasks))
    
    print(res)
    
    
main()
 """
import requests

import asyncio
async def main():
    url = "https://duckduckgo.com/?q=hel&t=brave&ia=web"
    try:
        response = requests.get(url)
        print(type(response))
        return response
    except Exception as e:
        print(e)
        return None, 404

loop  = asyncio.get_event_loop()
res = loop.run_until_complete( asyncio.gather(main()))
res = res[0]
print(res)
