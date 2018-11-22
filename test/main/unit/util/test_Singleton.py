import unittest

from projectarchitecture.util.Singleton import Singleton


@Singleton
class MyFirstSingletonTestClass:
    pass

@Singleton
class MySecondSingletonTestClass:
    pass

@Singleton
class SingletonTestClassInitParameters:

    def __init__(self, _value):
        self._value = _value

class SingletonTest(unittest.TestCase):

    def test_should_return_true_when_instance_are_the_same(self):
        self.assertEqual(MyFirstSingletonTestClass.instance(), MyFirstSingletonTestClass.instance())
        self.assertEqual(MySecondSingletonTestClass.instance(), MySecondSingletonTestClass.instance())

    def test_should_return_false_when_instance_are_different(self):
        self.assertNotEqual(MyFirstSingletonTestClass.instance(), MySecondSingletonTestClass.instance())

    def test_should_return_true_when_instance_are_equals_with_parameters(self):
        self.assertEqual(SingletonTestClassInitParameters.instance(1), SingletonTestClassInitParameters.instance(1))
        self.assertEqual(SingletonTestClassInitParameters.instance(1), SingletonTestClassInitParameters.instance(2))

    def test_should_raise_error_when_try_to_instance_singleton(self):
        with self.assertRaises(TypeError):
            SingletonTestClassInitParameters()

    def test_should_validate_method_isinstance_with_singleton(self):
        self.assertTrue(isinstance(MyFirstSingletonTestClass.instance(), MyFirstSingletonTestClass))

if __name__ == "__main__":
    unittest.main()