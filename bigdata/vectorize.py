from dumbo import identitymapper


def mapper(key, value):
    """
    Parses footfall records and returns  grid_id, hour of the day and footfall

    Input format:
       Date,Time,Week_commencing,Grid_ID,Grid_Area,Total,at_home,at_work,visiting
       ,male,female,age_0_20,age_21_30,age_31_40,

    Output:
       key = Grid_id
       value = (hour, footfall_total)
    """
    toks = value.split(',')
    hour = int(toks[1].split(':')[0])

    yield toks[3], (hour, int(toks[5]))


def reducer(key, values):
    """
    Creates a day vector, one slot per hour, and fill it with the records
    received
    """
    my_vector = [0] * 24

    for v in values:
        my_vector[v[0]] = v[1]
    yield 0, (sum(my_vector), key, my_vector)


def sort_reducer(key, values):
    """
    Sort values received (first element is the sum) and returns top one
    """
    yield sorted(values, reverse=True)[0], ""


# Job Full Workflow
def runner(job):
    opts = [("inputformat", "text"),
            ("outputformat", "sequencefile"),
            ]

    o1 = job.additer(mapper,
                     reducer, opts=opts)

    opts = [("inputformat", "sequencefile"),
            ("outputformat", "text"),
            ]

    o2 = job.additer(identitymapper,
                     sort_reducer, opts=opts)

if __name__ == "__main__":
    from dumbo import main
    main(runner)
