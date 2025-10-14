import sys
import os

# Добавляем пути к модулям
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'aw-core'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'aw-server'))

from aw_server import main

main()