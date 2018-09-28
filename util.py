import os
import shutil
import sys
reload(sys)  
sys.setdefaultencoding('latin1')
def read_until(stream, separator, buffer_size=32*1024):
    datalist = []
    done = False
    while not done:
        chunk = stream.read(buffer_size)
        done = len(chunk) == 0
        while separator in chunk:
            i = chunk.find(separator)
            datalist.append(chunk[:i+1])
            yield ''.join(datalist)
            datalist = []
            if i < len(chunk) - 1:
                chunk = chunk[i + 1:len(chunk)]
            else:
                chunk = ''

        if chunk != '':
            datalist.append(chunk)

    if len(datalist) > 0:
        yield ''.join(datalist)

    return

def groupsgen(seq, size):
    it = iter(seq)
    while True:
        values = ()
        for n in xrange(size):
            try:
                values += (it.next(),)
            except StopIteration:
                yield values
                return
        yield values

def create_folder_temp(path_temp):
    existTemp = os.path.exists(path_temp)
    if existTemp:
        print ('Temp folder already exist')
        shutil.rmtree(path_temp)
        print ('Temp folder removed')
    os.makedirs(path_temp)
    print ('Temp folder created')



def write_outputs(line):
    print 'geoimporter: writing in file output.txt', line
    f= open("output.txt","a+")
    f.write(str(line)+"\n\n")
    f.close()

def redefine_name(name):
    try:       
        new_name = ''
        longitud = len(name)
        split_name = name.split('_')
        maybe_number = split_name[len(split_name)-1]

        number = int(maybe_number)
        next_number = int(number) + 1
        divisor_unidad = name[longitud - 2] == '_'
        divisor_decenas = name[longitud - 3] == '_'
        print divisor_unidad, divisor_decenas
        if divisor_unidad:
            new_name = name[:longitud - 1] + str(next_number)
        if divisor_decenas:
            new_name = name[:longitud - 2] + str(next_number)
    except:
      new_name = name + '_' + '1'
    finally:
      write_outputs( 'geoimporter: Info shape renamed from ' + name +'  to ' + new_name)
      print 'geoimporter: Info shape ' + name +' rename to ' + new_name
      return new_name

