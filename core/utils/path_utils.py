
import os



def solve_dir(dirname):


    if not os.path.exists(dirname):
        os.mkdir(dirname)

    return os.path.abspath(dirname)

def solve_path(path, parent=None):

    if parent:
        
        parent = solve_dir(parent)
        path = os.path.join(parent, path)
    
    return os.path.abspath(path)


def lst_files(dirname, extension):

    files = [solve_path(f, dirname)
            for f in os.listdir(dirname)
            if f.lower().endswith(extension)]

    return files

