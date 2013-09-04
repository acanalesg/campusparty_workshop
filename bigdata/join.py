from dumbo import MultiMapper, identitymapper, JoinReducer
from dumbo.decor import primary, secondary


def mapper_footfalls(key, value):
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


def mapper_locations(key, value):
    """ Parses grid_id reference dataset.
    Input format:
        ID,LONCENTROI,LATCENTROI,AREASQKM

    Output:
       key = grid_id
       value = latlon
    """
    toks = value.split(',')
    yield toks[0], (toks[1], toks[2])


class Joiner(JoinReducer):
    """ Join datasets, receives grid reference as primary
    and footfall vector as secondary.
    Shared key is grid_id
    """
    def primary(self, key, values):
        self.latlon = values.next()

    def secondary(self, key, values):
        footfall = [0] * 24
        for v in values:
            footfall[v[0]] += int(v[1])
        yield sum(footfall), (self.latlon, footfall)


# Job Full Workflow
def runner(job):
    opts = [("inputformat", "text"),
            ("outputformat", "text"),
            ]

    multimap = MultiMapper()
    multimap.add("outputgrid", primary(mapper_locations))
    multimap.add("metropolitan", secondary(mapper_footfalls))

    o1 = job.additer(multimap,
                     Joiner, opts=opts)


if __name__ == "__main__":
    from dumbo import main
    main(runner)
