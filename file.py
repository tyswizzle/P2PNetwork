class File:
    def __init__(self, name, mod=0, path='Master'):
        self.name = name
        self.path = path
        self.mod = mod

    def __str__(self):
        ret = ''
        ret = ret + "Name: " + str(self.name) + "\nPath: " + str(self.path) + "\nMod: " + str(self.mod)
        print(ret)
        return ret
