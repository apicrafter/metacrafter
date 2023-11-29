#!/usr/bin/env python
import re
from collections import Counter, defaultdict
from functools import reduce

import typer


def collection_all_digits(c):
  'Returns True if all elements of the collection are digits.'
  return reduce(lambda x,y: x and y.isdigit(), c, True)

def collection_all_alpha(c):
  'Returns True if all elements of the collection are alpha.'
  return reduce(lambda x,y: x and y.isalpha(), c, True)

def has_cyrillic(c):
    return reduce(lambda x,y: x and bool(re.search('[а-яА-Я]', y)), c, True)

def length_range(c):
  """
  Returns a tuple with the minimum and maximum length of 
  elements in the collection.
  """
  l = list(map(len, c))
  return (min(l), max(l))

def id_to_regex(id):
  x = re.escape(id)
  x = re.sub(r'([^А-ЯA-Z0-9]+)', r'(\1)', x)
  x = re.sub(r'[A-Z]+', r'([A-Z)]+)', x)
  x = re.sub(r'[0-9]+', r'([0-9]+)', x)
  x = re.sub(r'[А-Я]+', r'([А-Я)]+)', x)
  return '^' + x + '$'

def make_histograms(identifiers):
  patterns = defaultdict(set)
  histograms = []

  for id in identifiers:
    pattern = id_to_regex(id)
    patterns[pattern].add(id)

  for pattern, identifiers in patterns.items():
    matches = [re.match(pattern, id)
               for id in identifiers
               if re.match(pattern, id)]

    histogram = defaultdict(Counter)

    for match in matches:
      for n_group, text in enumerate(match.groups()):
        histogram[n_group][text] += 1

    histograms.append(histogram)

  return histograms

def histogram_to_regex(histogram, collapse_threshold):
  """
  Takes a histogram, which is a dictionary of Counters, and converts
  it to a regular expression.
  """

  sorted_histogram = sorted(list(histogram.items()),
                            key=lambda x: x[0])
  regex_groups = []

  for group, values in sorted_histogram:
    group_all_digits = collection_all_digits(values)
    group_all_alpha = collection_all_alpha(values)
    is_cyrillic = has_cyrillic(values)

    n = len(values)
    min,max = length_range(values)

    if min == 1 and max == 1:
      range_spec = ''
    elif min == max:
      range_spec = '{%d}' % min
    else:
      range_spec = '{%d,%d}' % (min,max)
    if group_all_digits:
        char_spec = '[0-9]'
    elif group_all_alpha and not is_cyrillic:
        char_spec = '[A-Z]'
    else:
        char_spec = '[А-Я]'
    if group_all_digits and max <= 1:
      regex_groups.append('(%s)' % '|'.join(values.keys()))
    elif n <= collapse_threshold and not group_all_digits:
      # Substitute the unique values for this group into the regex,# enclosing in parentheses if more than one
      escaped_keys = [re.escape(x) for x in values.keys()]
      mask = '(%s}' if n > 1 else '%s'
      regex_groups.append(mask % '|'.join(escaped_keys))
    else:
      regex_groups.append(char_spec + range_spec)

  return ''.join(regex_groups)


def make_regex(identifiers, collapse_threshold=5):
  hs = make_histograms(identifiers)
  for h in hs:
    print(histogram_to_regex(h, collapse_threshold))


def process(filename):
  f = open(filename, 'r', encoding='utf8')
  data = alist = [line.rstrip() for line in f]
  f.close()
  make_regex(data)



if __name__ == "__main__":
  typer.run(process)