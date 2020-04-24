from transformation import Transformation
from load import Load
import sys

def main():

    if len(sys.argv) != 3:
        print('Error: Execution -> python3 main.py <url> <name_database>')
        exit(1)

    url = sys.argv[1]
    name_db = sys.argv[2]

    transformation = Transformation(url=url, output_path='databases/', name_db=name_db)
    transformation.transformation()

    load = Load(transformation.new_engine)
    load.load(output_path='excel/')

if __name__ == "__main__":
    main()
