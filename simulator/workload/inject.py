import sys # argv
import random # random

old_log = sys.argv[1]
new_log = sys.argv[2]

def inject():
    f_old = open(old_log)
    f_new = open(new_log, 'w')

    for line in f_old:
        line = line.strip('\n')
        line += '\t' + str(random.random()) + '\t' + str(random.random()) + '\n'
        f_new.write(line)

    f_new.close()

def main():
    inject()

if __name__ == '__main__':
    main()
