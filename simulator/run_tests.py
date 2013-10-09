import os
import subprocess

TEST_PATH="./tests/"

def main():
    for f in os.listdir(TEST_PATH):
        if '__init__' in f or not f.endswith(".py"):
            continue
        subprocess.call(["python", "-m", "tests." + f.strip(".py")])

if __name__ == '__main__':
    main()
