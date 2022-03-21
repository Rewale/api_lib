class ServiceNotFound(Exception):
    pass


class ParamNotFound(Exception):
    pass


class RequireParamNotSet(Exception):
    pass


class ServiceMethodNotAllowed(Exception):
    pass


class MethodNotFound(Exception):
    pass


class ParamValidateFail(Exception):
    pass


class ParamNotExist(Exception):
    pass


class MethodNotSet(Exception):
    def __init__(self, name_method):
        self.name = name_method

    def __str__(self):
        return f'Не задан обработчик для метода {self.name}'


class MethodsNotSet(Exception):
    def __str__(self):
        return f'Не заданы обработчики для методов сервиса'
