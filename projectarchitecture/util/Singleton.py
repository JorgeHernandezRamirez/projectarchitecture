class Singleton:

    def __init__(self, _decorated):
        self._decorated = _decorated

    def instance(self, *args, **kwargs):
        try:
            return self._instance
        except AttributeError:
            self._instance = self._decorated(*args, **kwargs)
            return self._instance

    def clean(self):
        try:
            del self.__dict__['_instance']
        except KeyError:
            pass

    def __call__(self):
        raise TypeError('Los singletons debe ser accedidos a través del método instance.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._decorated)

