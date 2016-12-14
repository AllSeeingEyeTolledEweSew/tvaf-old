import argparse
import btn

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("positive_cmd")
    parser.add_argument("negative_cmd")

    api = btn.API()
