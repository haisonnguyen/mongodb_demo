# Haison Nguyen
# CS 486 mongodb demo
# Pokemon database implemented using PyMongo, a python driver for MongoDB

from pymongo import MongoClient
import re
from bson.objectid import ObjectId
from pprint import pprint

###################################################################################################################################################
def main():
    
    # Establishes a connection to mongo server
    client = MongoClient('localhost', 27017)

    # Creates database 'pokemongo'
    db = client['pokemongo']

    # Collection of Pokemon
    pokemon = db['pokemon']

    # Collection of Skills 
    skills = db['skills']

    # Loads file and stores into pokemon and skills
    load_file('pokemongo.csv', pokemon,skills)

###################################################################################################################################################

    # Pokemon collection
    # SELECT * FROM pokemon;
    res = pokemon.find({})
    print("\n\nPokemon Collection")
    for r in res:
        pprint(r)
        print('')

    # Skills collection 
    # SELECT * FROM skills;
    res = skills.find({})
    print("\n\nSkills Collection")
    for r in res:
        pprint(r)

    # IN SQL: SELECT * FROM pokemon where Name = "Bulbasaur" 
    # NO SQL
    print("\n\nFind a document in the Pokemon Collection where the name is 'Bulbasaur'")
    n = pokemon.find_one({'Name': {'$eq': 'Bulbasaur'}})
    pprint(n)

    # IN SQL: SELECT * FROM POKEMON where Type = "Fire"
    print("\n\nFire type pokemon")
    fires = pokemon.find({'Type': {'$eq': 'Fire'}})
    for f in fires:
        pprint(f)

    # IN SQL: SELECT * FROM POKEMON where Attack is > 120 and Attack is < 160
    print("\n\nFind all pokemon documents where the attack is greater then 125 but less then 160")
    gtlt = pokemon.find({
        'Attack': {
            '$gt': 125, # greater than operator
            '$lte': 160 # lte or equal to
        }
    })
    # Print results of greater than less than query
    for p in gtlt:
        pprint(p)

    # Delete all documents in a collection
    skills.delete_many({})
    pokemon.delete_many({})

###################################################################################################################################################
###################################################################################################################################################

def load_file(filename, pokemon, skills):
    with open(filename, 'r') as f:
        lines = f.read().split('\n')
        # Store headers then get rid of the line from mem
        headers = lines[0].rstrip().split(',')
        del lines[0]
        # Pattern matching using regex
        s = r'(\d+),([A-Za-z]+),([A-Za-z]+),(\d+),(\d+),(\d+),(\d+)\%,(\d+)\%,(\d*),"*([A-Za-z\,\s\-]*)"*,"*([A-Za-z\,\s\-]*)"*'
        pokemons = list()
        # For each entry, pattern match each attribute and store as a list inside a list
        for line in lines:
            res = re.search(s, line).groups()
            pokemons.append(list(res))
        # Parse each document and insert
        for p in pokemons:
            d = {}
            for i in range(len(p)):
                if not p[i] == '':
                    moves = []
                    # If there are any moves we don't have in the DB we need to add it to the skills collection first
                    # then embed references to that skill inside a list of this pokemon document
                    if i == 9 or i == 10: # if the index currently in question is quick_moves or special_moves
                        moves_ = p[i].split(', ') # list of moves
                        for move in moves_:
                            moves.append(insert_skill(skills, move))
                        d[headers[i]] = moves
                    else:
                        try:
                            d[headers[i]] = int(p[i])
                        except ValueError:
                            d[headers[i]] = p[i]
            insert_pokemon(pokemon, d)


# add skills and return id of the skill doc being inserted or return id of result
# str(ObjectId(id)) strips id as a raw str
def insert_skill(col, skill):
    res = col.find_one({'Name': { '$eq': skill }})
    if not res: # no match
        i = col.insert_one({'Name': skill })
        return str(i.inserted_id)
    else: # match
        return str(res[u'_id'])

# Inserts if a pokemon document doesn't already exist
def insert_pokemon(col, p):
    res = col.find_one({'Name': { '$eq': p['Name'] }})
    if not res:
        col.insert_one(p)

if __name__ == '__main__':
    main()
