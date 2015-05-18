from collections import namedtuple
from datetime import datetime, timedelta
from pytz import utc as utc_tz

__author__ = 'Scott Lessans'

DATE_TIME_MIN = datetime.min.replace(tzinfo=utc_tz)
DATE_TIME_MAX = datetime.max.replace(tzinfo=utc_tz)


def beginning_of_day(dt: datetime) -> datetime:
    """
    Returns a data object representing beginning of day on the date specified.
    Here beginning is defined as 0 hours, minutes, second, microseconds after day
    starts.
    """
    time_into_day = timedelta(hours=dt.hour,
                              minutes=dt.minute,
                              seconds=dt.second,
                              microseconds=dt.microsecond)
    return dt - time_into_day


def _check_valid_aware_datetime(dt):
    """
    Checks that the argument is a valid instance of datetime and that it is not naive
    """
    if not isinstance(dt, datetime):
        raise ValueError('datetime expected')

    if dt.tzinfo is None:
        raise ValueError('DateTimeInterval cannot handle naive datetimes.')


def time_intervals_between(start: datetime, end: datetime, interval_length: timedelta, limit_to_end=False):
    """
    Generator for DateTimeIntervals of length interval_length during this time period.

    If limit_by_end is True, the end date of the last time period will not exceed self.end even if
    it causes the final interval to be shorter than interval_length.

    If limit_by_end is False, the last interval will be interval_length even if it exceeds self.end
    """
    while start < end:
        interval_end = start + interval_length

        if limit_to_end and interval_end > end:
            interval_end = end

        yield DateTimeInterval(start, interval_end)

        start = interval_end


def days_between(start_of: datetime, end_of: datetime):
    """
    Generator for intervals of length 1 day. They will start at the beginning of
    start_of -- that is, beginning_of_day(start_of) and end at the end of end_of,
    that is beginning_of_day(end + timedelta(days=1)). The datetimes either must both be
    naive, or must have the same timezone.
    """
    if start_of.tzinfo != end_of.tzinfo:
        raise ValueError("start_of and end_of must either have same timezone or both be naive.")

    return time_intervals_between(
        beginning_of_day(start_of),
        beginning_of_day(end_of + timedelta(days=1)),
        timedelta(days=1)
    )


def intersection_of_intervals(intervals1, intervals2):
    """
    intervals1 and intervals2 must be iterable or collection of non-overlapping DateTimeInterval objects
    in strictly ascending order.

    Returns generator x of ascending non-overlapping intervals i such that for all i in x
    i is contained by some interval in intervals1 and i is contained by some interval in intervals2.
    """

    i1 = next(intervals1)
    i2 = next(intervals2)

    while i1 is not None and i2 is not None:

        if i1.is_before(i2):
            i1 = next(intervals1)
            continue

        if i1.is_after(i2):
            i2 = next(intervals2)
            continue

        # if not before and not after, these are necessary overlapping
        # calculate overlap and yield
        assert i1.overlaps(i2)
        overlap = i1.overlap(i2)
        yield overlap

        # next we must increment at least or we'll be in an infinite loop

        # a) it cant be true that i1.end < overlap.end or overlap is invalid (contains time not in i1)
        # b) it cant be true that i2.end < overlap.end or overlap is invalid (contains time not in i2)
        # c) it cant be true that both end after overlap, or overlap could be extended
        # d) (a) and (b) and (c) --> i1.end == overlap.end or i2.end == overlap.end
        assert i1.end >= overlap.end  # a)
        assert i2.end >= overlap.end  # b)
        assert not (i1.end > overlap.end and i2.end > overlap.end)  # c)

        # this one is actually implied by the previous ones, but is important since it prevents
        # infinite loops
        assert (i1.end == overlap.end or i2.end == overlap.end)  # d)

        if i1.end == overlap.end:
            i1 = next(intervals1)

        if i2.end == overlap.end:
            i2 = next(intervals2)


class DateTimeInterval(object):
    """
    Represents an immutable period of time between two dates. The start date is strictly
    less than the end date. The period is inclusive of the start date and exclusive of the end date.

    The time interval ALWAYS stores non-naive datettimes ie they MUST have associated timezones.
    """

    def __init__(self, start: datetime, end: datetime):
        # according to python 3 docs https://docs.python.org/3.4/library/datetime.html:
        # "Objects of these types are immutable." (referring to datetime among others)
        # so we don't have to copy or worry about them changing on us

        if not start:
            start = DATE_TIME_MIN

        if not end:
            end = DATE_TIME_MAX

        _check_valid_aware_datetime(start)
        _check_valid_aware_datetime(end)

        # convert to UTC if not already
        self._start = start
        self._end = end

        if self._end < self._start:
            raise ValueError('end must be >= start', self._start, self._end)

    @classmethod
    def create_with_length(cls, start: datetime, diff: timedelta):
        return cls(start, start + diff)

    @property
    def is_start_infinite(self):
        return self.start == DATE_TIME_MIN

    @property
    def is_end_infinite(self):
        return self.end == DATE_TIME_MAX

    @property
    def is_infinite(self):
        return self.is_start_infinite or self.is_end_infinite

    @property
    def start(self) -> datetime:
        return self._start

    @property
    def end(self) -> datetime:
        return self._end

    @property
    def length(self) -> timedelta:
        if self.is_infinite:
            return None
        return self.end - self.start

    def contains(self, dt: datetime) -> bool:
        """
        If this moment is contained by this time interval
        :param dt:
        :return:
        """
        _check_valid_aware_datetime(dt)
        return self.start <= dt < self.end

    def covers(self, time_interval) -> bool:
        """
        Returns true if the passed time interval is completely covered by this time interval.
         Note that this is true if the ends dates are equal. since they aren't part of the interval, if
          the end dates are equal then all instances of the passed time zone still have a corresponding instance
          in this time zone.
        :param time_interval:
        :return:
        """
        # NOTE: the end date check is inclusive (since it would still be valid if end dates are equal) thus
        # we can just use contains for that
        return self.contains(time_interval.start) and (self.start <= time_interval.end <= self.end)

    def overlaps(self, time_interval) -> bool:
        """
        True if any instants of the passed time interval are in this time interval
        :param time_interval:
        :return:
        """
        return time_interval.start < self.end and self.start < time_interval.end

    def overlap(self, time_interval):
        if not self.overlaps(time_interval):
            return None

        return DateTimeInterval(
            start=max(self.start, time_interval.start),
            end=min(self.end, time_interval.end)
        )

    def is_before(self, other) -> bool:
        return self.end <= other.start

    def is_after(self, other) -> bool:
        return other.is_before(self)

    def ends_before(self, dt: datetime):
        return self.end <= dt

    def ends_after(self, dt: datetime):
        return self.end > dt

    def starts_after(self, dt: datetime):
        return self.start > dt

    def starts_before(self, dt: datetime):
        return self.start < dt

    def __eq__(self, other):
        if other is self:
            return True
        if not isinstance(other, DateTimeInterval):
            return False
        return self.start == other.start and self.end == other.end

    def __str__(self, *args, **kwargs):
        return self.debug_str(False)

    def debug_str(self, convert_to_utc=True):
        if self.is_start_infinite:
            start = '-inf'
        else:
            start = (self.start.astimezone(utc_tz) if convert_to_utc else self.start).isoformat(' ')

        if self.is_end_infinite:
            end = '+inf'
        else:
            end = (self.end.astimezone(utc_tz) if convert_to_utc else self.end).isoformat(' ')

        return "<%s: [%s, %s)>" % (type(self), start, end,)

    def intervals(self, interval_length: timedelta, limit_by_end=True):
        """
        see time_intervals_between for argument info
        """
        if self.is_infinite:
            # TODO correct exception type
            raise ValueError("cannot iterate over infinite interval")
        return time_intervals_between(self.start, self.end, interval_length, limit_by_end)


# represents an interval with one status throughout.
_SingleStatusInterval = namedtuple('_SingleStatusInterval', ['status', 'interval'])


def _non_overlapping_intervals(of_interval: DateTimeInterval, with_interval: DateTimeInterval):
    """
    Returns intervals from of_interval that do not overlap with with_interval.
    """
    before = None
    after = None

    if with_interval.start > of_interval.start:
        before = DateTimeInterval(of_interval.start, with_interval.start)

    if of_interval.end > with_interval.end:
        after = DateTimeInterval(with_interval.end, of_interval.end)

    return before, after


def _smooth_status_intervals(status_intervals):
    """
    Finds intervals that abut with the same status and turns them into one larger interval
    """
    smoothed = []

    last_status_interval = None
    for status_interval in status_intervals:

        assert last_status_interval is None or last_status_interval.interval.end <= status_interval.interval.start

        if last_status_interval and last_status_interval.status == status_interval.status and \
                        last_status_interval.interval.end == status_interval.interval.start:

            status_interval = _SingleStatusInterval(
                interval=DateTimeInterval(last_status_interval.interval.start, status_interval.interval.end),
                status=last_status_interval.status
            )

        elif last_status_interval:
            smoothed.append(last_status_interval)

        last_status_interval = status_interval

    if last_status_interval:
        smoothed.append(last_status_interval)

    return smoothed


class MultiStatusInterval(object):
    """
    This class represents an interval of time in which each moment has exactly one status (None is a
    valid status).  In other words, a function (in the mathematical sense) mapping time to status over
    a particular domain (i.e., "interval").  The status options are represented by arbitrary strings or
    None for no status.

    Complexity:
    Let n be the number of "status changes" within the interval.  (I.e., the size of a minimal
    partitioning of the interval such that each part of the partition is an interval with a single status.)

    This class has O(n) memory complexity.
    The class has O(n) runtime complexity for tge following:
        - mark an arbitrary interval with a certain status
        - check the status of an arbitrary moment

    Every other method runs in sub O(n)

    EASY TODO: make memory complexity constant [AT: constant with respect to what?]
    HARD TODO: make runtime complexity constant
        [AT: intuitively, seems to me like status() and mark() are currently at a balance
        point of O(n) runtime, and reducing one would mean increasing the other... but I
        could easily be missing something subtle.]
    """

    def __init__(self, interval: DateTimeInterval):
        if not isinstance(interval, DateTimeInterval):
            raise ValueError("interval must be instance of DateTimeInterval")

        self._interval = interval

        # list of non-overlapping, sequential, ascending, _SingleStatusInterval objects in
        self._intervals = []

    @property
    def interval(self) -> DateTimeInterval:
        return self._interval

    def status(self, dt: datetime) -> str:
        if self.interval.contains(dt):
            for status_interval in self._intervals:
                # since intervals are non-overlapping and ascending, if this interval starts AFTER the moment
                # in question then no other interval could possible contain this
                if status_interval.interval.starts_after(dt):
                    break

                # check if this interval contains the instant. if it does, we're done
                if status_interval.interval.contains(dt):
                    return status_interval.status

        return None

    def intervals_with_status(self, status):
        """
        Returns DateTimeIntervals where status is status. Returned intervals
        will be ascending, non-overlapping.
        """
        return (si.interval for si in self._intervals if si.status == status)

    def mark(self, interval: DateTimeInterval, status: str):
        if not isinstance(status, (str, bytes)):
            raise ValueError("status must be string")

        interval = self.interval.overlap(interval)
        if interval:
            self._mark(_SingleStatusInterval(interval=interval, status=status))

    def _mark(self, to_add: _SingleStatusInterval):

        intervals = []

        did_add = False
        for status_interval in self._intervals:

            # 6 possibilities:
            #   1) to_add is completely after this interval
            #   2) to_add is completely before this interval
            #   3) to_add covers status_interval
            #   4) to_add is covered by status_interval
            #   5) status_interval starts before to_add and ends during it
            #   6) status_interval starts during to_add and ends after it

            if to_add.interval.is_after(status_interval.interval):  # case (1)
                # this interval comes before to_add, just add it
                intervals.append(status_interval)

            elif to_add.interval.is_before(status_interval.interval):  # case (2)
                # to_add is completely before this interval, so we can just add it
                if not did_add:
                    intervals.append(to_add)
                    did_add = True

                intervals.append(status_interval)

            else:  # case (3), (4), (5), (6)
                assert status_interval.interval.overlaps(to_add.interval)

                before, after = _non_overlapping_intervals(status_interval.interval, to_add.interval)

                if before:
                    assert not did_add
                    intervals.append(_SingleStatusInterval(interval=before, status=status_interval.status))

                if not did_add:
                    intervals.append(to_add)
                    did_add = True

                if after:
                    assert did_add
                    intervals.append(_SingleStatusInterval(interval=after, status=status_interval.status))

        if not did_add:
            intervals.append(to_add)

        self._intervals = _smooth_status_intervals(intervals)