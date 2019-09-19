
import abc

class Market(object):
    STOCK = 1
    FUTURES = 2
    COIN = 3
    FEXCHANGE = 4
    CHANLUN = 5

class Frequency(object):
    """Enum like class for bar frequencies. Valid values are:

    * **Frequency.TRADE**: The bar represents a single trade.
    * **Frequency.SECOND**: The bar summarizes the trading activity during 1 second.
    * **Frequency.MINUTE**: The bar summarizes the trading activity during 1 minute.
    * **Frequency.HOUR**: The bar summarizes the trading activity during 1 hour.
    * **Frequency.DAY**: The bar summarizes the trading activity during 1 day.
    * **Frequency.WEEK**: The bar summarizes the trading activity during 1 week.
    * **Frequency.MONTH**: The bar summarizes the trading activity during 1 month.
    """

    # It is important for frequency values to get bigger for bigger windows.
    TRADE = -1
    SECOND = 1
    MINUTE = 60
    MINUTE5 = 5 * 60
    MINUTE30 = 30 * 60
    HOUR = 60 * 60
    HOUR2 = 2 * 60 * 60
    DAY = 24 * 60 * 60
    WEEK = 24 * 60 * 60 * 7
    MONTH = 24 * 60 * 60 * 31
    QUARTER = 24 * 60 * 60 * 31 * 3  # don't need to precise


class Bar(object):
    """A Bar is a summary of the trading activity for a security in a given period.

    .. note::
        This is a base class and should not be used directly.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def setUseAdjustedValue(self, useAdjusted):
        raise NotImplementedError()

    @abc.abstractmethod
    def getUseAdjValue(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def getDateTime(self):
        """Returns the :class:`datetime.datetime`."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getOpen(self, adjusted=False):
        """Returns the opening price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getHigh(self, adjusted=False):
        """Returns the highest price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getLow(self, adjusted=False):
        """Returns the lowest price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getClose(self, adjusted=False):
        """Returns the closing price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getVolume(self):
        """Returns the volume."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getAdjClose(self):
        """Returns the adjusted closing price."""
        raise NotImplementedError()

    @abc.abstractmethod
    def getFrequency(self):
        """The bar's period."""
        raise NotImplementedError()

    def getTypicalPrice(self):
        """Returns the typical price."""
        return (self.getHigh() + self.getLow() + self.getClose()) / 3.0

    @abc.abstractmethod
    def getPrice(self):
        """Returns the closing or adjusted closing price."""
        raise NotImplementedError()

    def getExtraColumns(self):
        return {}


class BasicBar(Bar):
    # Optimization to reduce memory footprint.
    __slots__ = (
        '__dateTime',
        '__open',
        '__close',
        '__high',
        '__low',
        '__volume',
        '__adjClose',
        '__frequency',
        '__useAdjustedValue',
        '__extra',
    )

    def __init__(self, dateTime, open_, high, low, close, volume, adjClose, frequency, extra={}):

        if high < low:
            raise Exception("high < low on %s" % (dateTime))
        elif high < open_:
            raise Exception("high < open on %s" % (dateTime))
        elif high < close:
            raise Exception("high < close on %s" % (dateTime))
        elif low > open_:
            raise Exception("low > open on %s" % (dateTime))
        elif low > close:
            raise Exception("low > close on %s" % (dateTime))

        self.__dateTime = dateTime
        self.__open = open_
        self.__close = close
        self.__high = high
        self.__low = low
        self.__volume = volume
        self.__adjClose = adjClose
        self.__frequency = frequency
        self.__useAdjustedValue = False
        self.__extra = extra

    def __setstate__(self, state):
        (self.__dateTime,
         self.__open,
         self.__close,
         self.__high,
         self.__low,
         self.__volume,
         self.__adjClose,
         self.__frequency,
         self.__useAdjustedValue,
         self.__extra) = state

    def __getstate__(self):
        return (
            self.__dateTime,
            self.__open,
            self.__close,
            self.__high,
            self.__low,
            self.__volume,
            self.__adjClose,
            self.__frequency,
            self.__useAdjustedValue,
            self.__extra
        )

    def setUseAdjustedValue(self, useAdjusted):
        if useAdjusted and self.__adjClose is None:
            raise Exception("Adjusted close is not available")
        self.__useAdjustedValue = useAdjusted

    def getUseAdjValue(self):
        return self.__useAdjustedValue

    def getDateTime(self):
        return self.__dateTime

    def getOpen(self, adjusted=False):
        if adjusted:
            if self.__adjClose is None:
                raise Exception("Adjusted close is missing")
            return self.__adjClose * self.__open / float(self.__close)
        else:
            return self.__open

    def getHigh(self, adjusted=False):
        if adjusted:
            if self.__adjClose is None:
                raise Exception("Adjusted close is missing")
            return self.__adjClose * self.__high / float(self.__close)
        else:
            return self.__high

    def getLow(self, adjusted=False):
        if adjusted:
            if self.__adjClose is None:
                raise Exception("Adjusted close is missing")
            return self.__adjClose * self.__low / float(self.__close)
        else:
            return self.__low

    def getClose(self, adjusted=False):
        if adjusted:
            if self.__adjClose is None:
                raise Exception("Adjusted close is missing")
            return self.__adjClose
        else:
            return self.__close

    def getVolume(self):
        return self.__volume

    def getAdjClose(self):
        return self.__adjClose

    def getFrequency(self):
        return self.__frequency

    def getPrice(self):
        if self.__useAdjustedValue:
            return self.__adjClose
        else:
            return self.__close

    def getExtraColumns(self):
        return self.__extra


class Bars(object):
    """A group of :class:`Bar` objects.

    :param barDict: A map of instrument to :class:`Bar` objects.
    :type barDict: map.

    .. note::
        All bars must have the same datetime.
    """

    def __init__(self, barDict):
        if len(barDict) == 0:
            raise Exception("No bars supplied")

        # Check that bar datetimes are in sync
        firstDateTime = None
        firstInstrument = None
        for instrument, currentBar in barDict.items():
            if firstDateTime is None:
                firstDateTime = currentBar.getDateTime()
                firstInstrument = instrument
            elif currentBar.getDateTime() != firstDateTime:
                raise Exception("Bar data times are not in sync. %s %s != %s %s" % (
                    instrument,
                    currentBar.getDateTime(),
                    firstInstrument,
                    firstDateTime
                ))

        self.__barDict = barDict
        self.__dateTime = firstDateTime

    def __getitem__(self, instrument):
        """Returns the :class:`pyalgotrade.bar.Bar` for the given instrument.
        If the instrument is not found an exception is raised."""
        return self.__barDict[instrument]

    def __contains__(self, instrument):
        """Returns True if a :class:`pyalgotrade.bar.Bar` for the given instrument is available."""
        return instrument in self.__barDict

    def items(self):
        return self.__barDict.items()

    def keys(self):
        return self.__barDict.keys()

    def getInstruments(self):
        """Returns the instrument symbols."""
        return self.__barDict.keys()

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` for this set of bars."""
        return self.__dateTime

    def getBar(self, instrument):
        """Returns the :class:`pyalgotrade.bar.Bar` for the given instrument or None if the instrument is not found."""
        return self.__barDict.get(instrument, None)


class BasicTick(object):
    # Optimization to reduce memory footprint.
    __slots__ = (
        '__dateTime',
        '__open',
        '__close',
        '__high',
        '__low',
        '__volume',
        '__amount',
        '__bp',
        '__bv',
        '__ap',
        '__av',
        '__preclose',
        '__new_price',
        '__bought_amount',
        '__sold_amount',
        '__bought_volume',
        '__sold_volume',
        '__frequency',
        '__extra',
        '__adjClose',
        '__useAdjustedValue',
    )

    def __init__(self, dateTime, open_, high, low, close, volume, amount, bp, bv, ap, av, preclose \
                 , new_price, bought_amount, sold_amount, bought_volume, sold_volume, frequency, extra={}):

        self.__dateTime = dateTime
        self.__open = open_
        self.__close = close
        self.__high = high
        self.__low = low
        self.__volume = volume
        self.__amount = amount
        self.__bp = bp
        self.__ap = ap
        self.__bv = bv
        self.__av = av
        self.__preclose = preclose
        self.__bought_amount = bought_amount
        self.__sold_amount = sold_amount
        self.__bought_volume = bought_volume
        self.__sold_volume = sold_volume
        self.__frequency = frequency
        self.__extra = extra
        self.__useAdjustedValue = False
        self.__adjClose = new_price

    def __setstate__(self, state):
        (self.__dateTime,
         self.__open,
         self.__close,
         self.__high,
         self.__low,
         self.__volume,
         self.__amount,
         self.__bp,
         self.__ap,
         self.__bv,
         self.__av,
         self.__preclose,
         self.__bought_amount,
         self.__sold_amount,
         self.__bought_volume,
         self.__sold_volume,
         self.__frequency,
         self.__adjClose,
         self.__extra) = state

    def __getstate__(self):
        return (self.__dateTime,
                self.__open,
                self.__close,
                self.__high,
                self.__low,
                self.__volume,
                self.__amount,
                self.__bp,
                self.__ap,
                self.__bv,
                self.__av,
                self.__preclose,
                self.__bought_amount,
                self.__sold_amount,
                self.__bought_volume,
                self.__sold_volume,
                self.__frequency,
                self.__adjClose,
                self.__extra)

    def getDateTime(self):
        return self.__dateTime

    def getOpen(self, adjusted=False):
        if adjusted:
            if self.__adjClose is None:
                raise Exception("Adjusted close is missing")
            return self.__adjClose * self.__open / float(self.__close)
        else:
            return self.__open

    def getHigh(self, adjusted=False):
        if adjusted:
            if self.__adjClose is None:
                raise Exception("Adjusted close is missing")
            return self.__adjClose * self.__high / float(self.__close)
        else:
            return self.__high

    def getLow(self, adjusted=False):
        if adjusted:
            if self.__adjClose is None:
                raise Exception("Adjusted close is missing")
            return self.__adjClose * self.__low / float(self.__close)
        else:
            return self.__low

    def getClose(self, adjusted=False):
        return self.__close

    def getVolume(self):
        return self.__volume

    def getAmount(self):
        return self.__amount

    def getFrequency(self):
        return self.__frequency

    def getBp(self):
        return self.__bp

    def getBv(self):
        return self.__bv

    def getAp(self):
        return self.__ap

    def getAv(self):
        return self.__av

    def getPreclose(self):
        return self.__preclose

    def getBoughtVolume(self):
        return self.__bought_volume

    def getBoughtAmount(self):
        return self.__bought_amount

    def getSoldVolume(self):
        return self.__sold_volume

    def getSoldAmount(self):
        return self.__sold_amount

    def getExtraColumns(self):
        return self.__extra

    def setUseAdjustedValue(self, useAdjusted):
        if useAdjusted and self.__adjClose is None:
            raise Exception("Adjusted close is not available")
        self.__useAdjustedValue = useAdjusted

    def getUseAdjValue(self):
        return self.__useAdjustedValue

    def getAdjClose(self):
        return self.__close

    def getPrice(self):
        return self.__close


class Ticks(object):
    """A group of :class:`Bar` objects.

    :param barDict: A map of instrument to :class:`Bar` objects.
    :type barDict: map.

    .. note::
        All bars must have the same datetime.
    """

    def __init__(self, barDict):
        if len(barDict) == 0:
            raise Exception("No bars supplied")

        # Check that bar datetimes are in sync
        firstDateTime = None
        firstInstrument = None
        for instrument, currentBar in barDict.items():
            if firstDateTime is None:
                firstDateTime = currentBar.getDateTime()
                firstInstrument = instrument
            elif currentBar.getDateTime() != firstDateTime:
                raise Exception("Bar data times are not in sync. %s %s != %s %s" % (
                    instrument,
                    currentBar.getDateTime(),
                    firstInstrument,
                    firstDateTime
                ))

        self.__barDict = barDict
        self.__dateTime = firstDateTime

    def __getitem__(self, instrument):
        """Returns the :class:`pyalgotrade.bar.Bar` for the given instrument.
        If the instrument is not found an exception is raised."""
        return self.__barDict[instrument]

    def __contains__(self, instrument):
        """Returns True if a :class:`pyalgotrade.bar.Bar` for the given instrument is available."""
        return instrument in self.__barDict

    def items(self):
        return self.__barDict.items()

    def keys(self):
        return self.__barDict.keys()

    def getInstruments(self):
        """Returns the instrument symbols."""
        return self.__barDict.keys()

    def getDateTime(self):
        """Returns the :class:`datetime.datetime` for this set of bars."""
        return self.__dateTime

    def getBar(self, instrument):
        """Returns the :class:`pyalgotrade.bar.Bar` for the given instrument or None if the instrument is not found."""
        return self.__barDict.get(instrument, None)
