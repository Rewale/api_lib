class ServiceNotFound(Exception):
    pass


class ParamNotFound(Exception):
    pass


class RequireParamNotSet(Exception):
    pass


class AllServiceMethodsNotAllowed(Exception):
    def __init__(self, name_service: str, service_from: str):
        self.name_service = name_service
        self.service_from = service_from

    def __str__(self):
        return f"Методы сервиса {self.name_service} недоступны из сервиса {self.service_from}"


class ServiceMethodNotAllowed(Exception):
    def __init__(self, name_service: str, name_method, service_from: str):
        self.name_method = name_method
        self.name_service = name_service
        self.service_from = service_from

    def __str__(self):
        return f"Метод {self.name_method} в сервисе {self.name_service} недоступен из сервиса {self.service_from}"


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
