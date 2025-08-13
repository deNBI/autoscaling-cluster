@total_ordering
class ScaleState(enum.Enum):
    """
    Possible program scaling states.
    """

    UP = 1
    DOWN = 0
    SKIP = 2
    DONE = 3
    DOWN_UP = 4
    FORCE_UP = 5
    DELAY = -1

    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented
