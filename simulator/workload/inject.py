import sys # argv
import random # random

old_log = sys.argv[1]
new_log = sys.argv[2]
num_rsrc = 8 # Number of CPUs/RAMs

def gen_rsrc():
    return int((random.random() * (num_rsrc - 1)) + 1)

def inject():
    f_old = open(old_log)
    f_new = open(new_log, 'w')

    for line in f_old:
        line = line.strip('\n')
        line += '\t' + str(gen_rsrc()) + '\t' + str(gen_rsrc()) + '\n'
        f_new.write(line)

    f_new.close()

def main():
    inject()

if __name__ == '__main__':
    main()
