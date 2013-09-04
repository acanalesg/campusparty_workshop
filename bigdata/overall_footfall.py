__author__ = 'acg'


def mapper(key, value):
    """
    Parses footfall records and returns just grid_id and footfall

    Input format:
       Date,Time,Week_commencing,Grid_ID,Grid_Area,Total,at_home,at_work,visiting
       ,male,female,age_0_20,age_21_30,age_31_40,

    Output:
       key = Grid_id
       value = footfall_total
    """
    toks = value.split(',')
    yield toks[3], int(toks[5])


def reducer(key, values):
    """
    Just sum all the footfalls received for the key, and return sum and grid id
    (you could use sumreducer instead, i.e from dumbo import sumreducer)
    """
    yield sum(values), key


# Job Full Workflow
def runner(job):
    opts = [("inputformat", "text"),
            ("outputformat", "text"),
            ]

    o1 = job.additer(mapper,
                     reducer, opts=opts)

if __name__ == "__main__":
    from dumbo import main
    main(runner)
