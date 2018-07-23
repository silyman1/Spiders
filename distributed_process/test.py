#-*-coding:utf-8-*-

from multiprocessing.managers import BaseManager

from multiprocessing import freeze_support,Process,Queue
queue = Queue()
class QueueManager(BaseManager): 
    pass
def test(queue):
    pass
def test2():
    QueueManager.register('get_queue', callable=lambda:queue)
    m = QueueManager(address=('127.0.0.1', 50000), authkey='abracadabra')
    s = m.get_server()
    url_process = Process(target=test,args=(queue,))
    url_process.start()
    print 'ok'
if __name__ == "__main__":
    freeze_support()
    test2()

#s.serve_forever()