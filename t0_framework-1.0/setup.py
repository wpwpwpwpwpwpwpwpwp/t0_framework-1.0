from distutils.core import setup

setup(
    name='t0_framework',
    version='1.0',
    description='alpha with cta mode backend',
    author='f.x zhenw.xu',
    author_email='sdu.xuefu@gmail.com',
    url='',
    license='sino quant group',
    platforms='python 3.6',
    packages=['cpa',
              'cpa.factorModel',
              'cpa.calculator',
              'cpa.factorPool',
              'cpa.factorProcessor',
              'cpa.feed',
              'cpa.config',
              'cpa.io',
              'cpa.indicators',
              'cpa.indicators.panelIndicators',
              'cpa.indicators.seriesIndicators',
              'cpa.resample',
              'cpa.utils']
)

