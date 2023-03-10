import requests
import threading
import time
import sys
import os

filedir = sys.argv[1]
position = int(sys.argv[2])
size = int(sys.argv[3])

path = '/home/ubuntu/PycharmProjects/Bokey_Project/PIML/skypham/' +filedir
if not os.path.exists(path):
    os.makedirs(path)

start_time = time.time()
print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))


filename = '/home/ubuntu/Downloads/ay_internal_data.csv'
prolog = "https://pubchem.ncbi.nlm.nih.gov/rest/pug"
exitFlag = 0


class myThread(threading.Thread):
    def __init__(self, threadID, name, num):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.num = num

    def run(self):
        print("Start:" + self.name)
        print_time(self.name, self.num, filename,position,size)
        print("Exit:" + self.name)


def print_time(threadName, num, filename,position,size):
    end = num * size + position
    start = end - size + 1
    print(position,start, end,threadName)
    output = path +'/position_' + str(start) + '_' + str(end) +'.csv'
    f = open(output, 'w')
    for idx, line in enumerate(open(filename, 'r')):
        if idx < start:
            continue
        if idx > end:
            break

        cid = str(line.strip('\n').strip(' '))

        smile = prolog + '/compound/cid/' + cid + '/property/CanonicalSMILES/txt'
        res2 = requests.post(smile)
        smiles = res2.text.strip()
        cas = prolog + '/compound/cid/' + cid + '/synonyms/txt'
        res3 = requests.post(cas)
        cas = res3.text.strip().split('\n')
        name = cas[0]
        name = 'NONE' if 'Status: 404' in name else name
        name = name[:11] if 'Status' in name else name
        CAS = [i for i in cas if (len(i) < 11 and i.count('-')) == 2 or ('CAS' in i and i.count('-') == 2)]


        CAS = CAS[0] if len(CAS) >= 1 else ''
        url = 'NONE' if smiles == 'NONE' else 'https://pubchem.ncbi.nlm.nih.gov/compound/' + cid
        f.write('%s\t%s\t%s\t%s\t%s\n' % (smiles, CAS, name, cid, url))
        if idx % 50 == 1:
            state = str((((idx - start + 1) / size) * 100))
            now_time = time.time()
            pred = ((now_time-start_time)*(1-float(state)/100))/(float(state)*36)
            print(threadName, "{}%".format(state),'pred_need_hours:',pred)
            time.sleep(0.3)


        if idx % 5 == 4:
            time.sleep(1)

    f.close()
    return (threadName, 'finish')

# line
thread1 = myThread(1, "Thread-1", 1)
thread2 = myThread(2, "Thread-2", 2)
thread3 = myThread(3, "Thread-3", 3)
thread4 = myThread(4, "Thread-4", 4)
thread5 = myThread(5, "Thread-5", 5)

# open
thread1.start()
thread2.start()
thread3.start()
thread4.start()
thread5.start()

thread1.join()
thread2.join()
thread3.join()
thread4.join()
thread5.join()

print("end")
end_time = time.time()
print('time', int(end_time - start_time))
print(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()))


## merge file
import pandas as pd
outfile = path+'/P'+filedir+'.csv'
filelist = os.listdir(path)
df = pd.read_csv(os.path.join(path,filelist[0]),sep='\t',header=None)
for file in filelist[1:]:
    dfi =pd.read_csv(os.path.join(path,file),sep='\t',header=None)
    df = pd.concat([df,dfi],ignore_index=True)

df.columns = ['Smiles','CAS','Name','CID','URL']
# noneed = [i for i in df['Smiles'].tolist() if 'Detail:' in i or 'Message:' in i or 'Code:' in i]
# df = df[~df['Smiles'].isin(noneed)]
df.to_csv(outfile,index = False)


print_file = path.replace(filedir,'')+'print_file.txt'
fp = open(print_file, 'a')
stderr = sys.stderr
stdout = sys.stdout
sys.stderr = fp
sys.stdout = fp
print('')
print('This procress is start at {} and end with {}'.format(position,position+size*5))
print('This process use time is', int(end_time - start_time))
print('This process create number is ',len(df))
print('This process ending time is',time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()),end='\n')
fp.close()
sys.stderr = stderr
sys.stdout = stdout

