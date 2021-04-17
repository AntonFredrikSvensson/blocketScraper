from cx_Freeze import setup, Executable

setup(name='Blocket Scrape',
        version='0.1',
        description='Scrape articles from Blocket',
        executables = [Executable("GeneralScraper.py")])