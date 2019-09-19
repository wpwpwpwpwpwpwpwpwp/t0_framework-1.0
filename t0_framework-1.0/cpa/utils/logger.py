'''
日志记录
@Time    : 2019/7/16 16:30
@Author  : Yanggang Fang
@Email   : ygangfang@outlook.com
'''

import logging
import threading

initLock = threading.Lock()
rootLoggerInitialized = False

# 输出时间、 logger名称、 触发logger的行号、 线程ID、 log级别、 信息
log_format = "%(asctime)s %(name)s %(lineno)d <%(thread)d> [%(levelname)s] %(message)s"
level = logging.INFO  # 将logger处理的最低级别设置为DEBUG
# file_log = "broker.log"  # 文件名， 默认为空
file_log = None  # 不存log日志
console_log = True  # 默认在console输出


def init_handler(handler):
    '''
    设置日志输出格式
    param handler: 日志处理器
    '''
    handler.setFormatter(Formatter(log_format))  # 设置logger输出的日志格式


def init_logger(logger):
    '''
    设置日志处理器及处理的日志最低级别
    param logger: 日志记录器
    '''
    logger.setLevel(level)  # 设置logger处理的日志最低级别

    # 当文件名不为空时调用FileHandler， 存入指定文件
    if file_log is not None:
        fileHandler = logging.FileHandler(file_log)
        init_handler(fileHandler)
        logger.addHandler(fileHandler)

    # 当文件名为空时调用StreamHandler, 在console输出
    if console_log:
        consoleHandler = logging.StreamHandler()
        init_handler(consoleHandler)
        logger.addHandler(consoleHandler)


def initialize():
    '''
    初始化日志记录器
    '''
    global rootLoggerInitialized
    with initLock:
        if not rootLoggerInitialized:
            init_logger(logging.getLogger())
            rootLoggerInitialized = True


def getLogger(name=None):
    '''
    为日志记录器设置名称，并完成初始化
    param name: 所要使用的日志记录器名称
    '''
    initialize()
    return logging.getLogger(name)


class Formatter(logging.Formatter):
    '''
    实现日志时间挂钩回测的数据对应时间
    param logging.Formatter: 继承logging库默认的日志格式化器类
    '''
    DATETIME_HOOK = None  # 默认为None, 不挂钩回测时间

    def formatTime(self, record, datefmt=None):
        newDateTime = None

        if Formatter.DATETIME_HOOK is not None:
            newDateTime = Formatter.DATETIME_HOOK()  # 挂钩回测时间
        if newDateTime is None:
            # 若不需要挂钩回测时间， 则使用logging库默认的formatTime设置时间格式
            ret = super(Formatter, self).formatTime(record, datefmt)
        else:
            ret = str(newDateTime)  # 挂钩回测时间,  将数据的时间设置为format的时间
        return ret
