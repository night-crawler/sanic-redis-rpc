#!/usr/bin/env python

from itertools import combinations
import random

from tqdm import tqdm
from redis import Redis

parts = {
    'knowledgeable',
    'true',
    'shivering',
    'nerve',
    'pear',
    'year',
    'shut',
    'possess',
    'typical',
    'diligent',
    'innate',
    'judge',
    'lighten',
    'share',
    'inconclusive',
    'believe',
    'fertile',
    'trap',
    'curtain',
    'chivalrous',
    'hate',
    'lock',
    'drum',
    'lumpy',
    'opposite',
    'subsequent',
    'loud',
    'lean',
    'hospital',
    'open',
    'rescue',
    'rod',
    'false',
    'elastic',
    'knit',
    'root',
    'store',
    'lopsided',
    'knee',
    'past',
    'popcorn',
    'quilt',
    'doubt',
    'imported',
    'delirious',
    'label',
    'mourn',
    'rejoice',
    'squalid',
    'provide',
    'creature',
    'reaction',
    'ignore',
    'vase',
    'ossified',
    'ignorant',
    'plant',
    'cactus',
    'excuse',
    'doctor',
    'kind',
    'inquisitive',
    'throne',
    'fit',
    'fire',
    'extra-small',
    'ducks',
    'sheep',
    'stimulating',
    'found',
    'motion',
    'smash',
    'yarn',
    'cover',
    'jar',
    'warlike',
    'mailbox',
    'long',
    'absorbed',
    'destruction',
    'drunk',
    'quarrelsome',
    'pencil',
    'alarm',
    'apparel',
    'silver',
    'obese',
    'hammer',
    'faded',
    'oil',
    'sense',
    'prickly',
    'venomous',
    'laughable',
    'juice',
    'spot',
    'helpful',
    'calculator',
    'coil',
    'bat',
}

parts = sorted(parts)

delimiters = ':/'

redis = Redis('0.0.0.0', 7000)
pipe = redis.pipeline()
key_comb = combinations(parts, 3)

print(f'preparing a pipe of ')
for i in tqdm(range(1000 * 1000)):
    try:
        comb = next(key_comb)
    except StopIteration:
        break

    key_ = random.choice(delimiters).join(comb)

    pipe.delete(key_)

    magic_number = random.randint(0, 9)
    # hash
    if 0 < magic_number < 2:
        bundle = {random.choice(parts): random.choice(parts) for k in range(len(parts))}
        pipe.hmset(key_, bundle)
        continue

    # sorted set
    if 2 < magic_number < 4:
        bundle = {random.choice(parts): random.randint(1, 10) for k in range(len(parts))}
        pipe.zadd(key_, **bundle)
        continue

    # set
    if 4 < magic_number < 6:
        pipe.sadd(key_, *parts)
        continue

    # list
    if 6 < magic_number < 8:
        pipe.lpush(key_, *parts)
        continue

    # key
    pipe.set(key_, 'here i am')


print('executing a pipe of 1kk')
res = pipe.execute()
print('done')

