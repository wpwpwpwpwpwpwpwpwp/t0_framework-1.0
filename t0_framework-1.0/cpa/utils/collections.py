import numpy as np
from pyalgotrade import observer
# Like a collections.deque but using a numpy.array.
class NumPyDeque(object):
    def __init__(self, maxLen, dtype=float):
        assert maxLen > 0, "Invalid maximum length"
        self.__values = np.empty(maxLen, dtype=dtype)
        self.__maxLen = maxLen
        self.__nextPos = 0

    def getMaxLen(self):
        return self.__maxLen

    def append(self, value):
        if self.__nextPos < self.__maxLen:
            self.__values[self.__nextPos] = value
            self.__nextPos += 1
        else:
            # Shift items to the left and put the last value.
            # I'm not using np.roll to avoid creating a new array.
            self.__values[0:-1] = self.__values[1:]
            self.__values[self.__nextPos - 1] = value

    def update(self, value):
        if self.__nextPos == 0:
            self.__values[self.__nextPos] = value
            self.__nextPos += 1
        else:
            self.__values[self.__nextPos - 1] = value

    def removeLast(self):
        if self.__nextPos > 0:
            self.__nextPos -= 1

    def data(self):
        # If all values are not initialized, return a portion of the array.
        if self.__nextPos < self.__maxLen:
            ret = self.__values[0:self.__nextPos]
        else:
            ret = self.__values
        return ret

    def resize(self, maxLen):
        assert maxLen > 0, "Invalid maximum length"

        # Create empty, copy last values and swap.
        values = np.empty(maxLen, dtype=self.__values.dtype)
        lastValues = self.__values[0:self.__nextPos]
        values[0:min(maxLen, len(lastValues))] = lastValues[-1*min(maxLen, len(lastValues)):]
        self.__values = values

        self.__maxLen = maxLen
        if self.__nextPos >= self.__maxLen:
            self.__nextPos = self.__maxLen

    def __len__(self):
        return self.__nextPos

    def __getitem__(self, key):
        return self.data()[key]

class ListDeque(object):
    def __init__(self, maxLen):
        assert maxLen > 0, "Invalid maximum length"

        self.__values = []
        self.__maxLen = maxLen

    def getMaxLen(self):
        return self.__maxLen

    def append(self, value):
        self.__values.append(value)
        # Check bounds
        if len(self.__values) > self.__maxLen:
            self.__values.pop(0)

    def data(self):
        return self.__values

    def updateLast(self, value):
        if self.__values.__len__() == 0:
            self.__values.append(value)
        else:
            self.__values[-1] = value

    def removeLast(self):
        self.__values.pop(-1)

    def resize(self, maxLen):
        assert maxLen > 0, "Invalid maximum length"

        self.__maxLen = maxLen
        self.__values = self.__values[-1 * maxLen:]

    def __len__(self):
        return len(self.__values)

    def __getitem__(self, key):
        return self.__values[key]

    def __setitem__(self, key, value):
        self.__values[key] = value

    def add(self, value):
        """
        :param value:
        :return:[] + value
        """
        return [i + value for i in self.__values]

#row * col panel Deque
class NumpyPanelDeque(object):
    def __init__(self, rowLen, colLen, dtype=np.float32):
        assert rowLen > 0 and colLen > 0, "Invalid maximum length"
        self.__values = np.empty((rowLen, colLen), dtype=dtype)
        self.__colLen = colLen
        self.__maxLen = rowLen
        self.__nextPos = [0]

    def getMaxLen(self):
        return self.__maxLen

    def getPositionReference(self):
        return self.__nextPos

    def getColumnsReference(self, idx):
        '''
        :return:此处列为传索引, 返回的sliceDeque
        '''
        return SliceDeque(self.__values[:, idx], self.__nextPos, self.__maxLen)

    def append(self, value):
        '''
        :param value: arrayLike value
        :return:
        '''
        assert len(value) == self.__colLen
        if self.__nextPos[0] < self.__maxLen:
            self.__values[self.__nextPos[0]] = value
            self.__nextPos[0] += 1
        else:
            # Shift items to the left and put the last value.
            # I'm not using np.roll to avoid creating a new array.
            self.__values[0:-1] = self.__values[1:]
            self.__values[self.__nextPos[0] - 1] = value

    def update(self, value):
        if self.__nextPos == 0:
            self.__values[self.__nextPos] = value
            self.__nextPos += 1
        else:
            self.__values[self.__nextPos[0] -1] = value

    def data(self):
        # If all values are not initialized, return a portion of the array.
        if self.__nextPos[0] < self.__maxLen:
            ret = self.__values[0:self.__nextPos[0]]
        else:
            ret = self.__values
        return ret

    def resize(self, rowLen, colLen):
        assert rowLen > 0 and colLen > 0, "Invalid maximum length"

        # Create empty, copy last values and swap.
        values = np.empty((rowLen, colLen), dtype=self.__values.dtype)
        lastValues = self.__values[0:self.__nextPos[0]]
        values[0:min(rowLen, len(lastValues)), 0: min(colLen, len(self.__colLen))] = lastValues[-1*min(rowLen, len(lastValues)):, 0: min(colLen, len(self.__colLen))]

        if colLen > self.__colLen:
            values[:, self.__colLen:] = np.nan
        self.__values = values
        self.__colLen = colLen

        self.__maxLen = rowLen
        if self.__nextPos[0] >= self.__maxLen:
            self.__nextPos[0] = self.__maxLen

    def __len__(self):
        return self.__nextPos[0]

    def __getitem__(self, key):
        '''
        :param key: [:, int]这种特殊情形传列索引sliceDeque对象,否则传值
        :return:
        '''
        if isinstance(key, tuple) and isinstance(key[1], int) and key[0] == slice(None, None, None):
            return self.getColumnsReference(key[1])

        return self.data()[key]

class SliceDeque(object):
    '''
    2-d array colume slice reference
    '''
    __slots__ = ('__values', '__maxLen', '__nextPos')

    def __init__(self,colValues, posPointer, maxLen):

        self.__values = colValues
        self.__maxLen = maxLen
        self.__nextPos = posPointer

    def getMaxLen(self):
        return self.__maxLen

    def data(self):
        # If all values are not initialized, return a portion of the array.
        if self.__nextPos[0] < self.__maxLen:
            ret = self.__values[0:self.__nextPos[0]]
        else:
            ret = self.__values
        return ret

    def __len__(self):
        return self.__nextPos[0]

    def __getitem__(self, key):
        return self.data()[key]