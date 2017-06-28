jobs = [(1, 1, '/usr/bin/Rscript --vanilla', './test_rapid_cycling.R', '', 50)] * 5000

def main():
    with open('test_rapid_cycling.txt', 'w') as f:
        for line in jobs:
            f.write(':'.join(tuple(map(str, line))) + '\n')

if __name__ == '__main__':
    main()








