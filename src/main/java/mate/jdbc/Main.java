package mate.jdbc;

import java.util.List;
import mate.jdbc.lib.Injector;
import mate.jdbc.model.Car;
import mate.jdbc.model.Driver;
import mate.jdbc.model.Manufacturer;
import mate.jdbc.service.CarService;

public class Main {
    private static final Injector injector = Injector.getInstance("mate.jdbc");

    public static void main(String[] args) {
        Car car = new Car("supra", new Manufacturer(4L, "Mini", "England"));
        car.setDrivers(List.of(new Driver(8L, "Harry", "2983")));

        CarService carService = (CarService) injector.getInstance(CarService.class);
        carService.getAll().forEach(System.out::println);
        System.out.println();

        Car car1 = new Car(2L, "Carolla", new Manufacturer(1L, "lskjdf", "slkdjf"));
        car1.addDriver(new Driver(7L, "slkdjf", "slkdjf"));
        car1.addDriver(new Driver(8L, "slkdjf", "slkdjf"));
        car1.addDriver(new Driver(9L, "slkdjf", "slkdjf"));

        carService.removeDriverFromCar(new Driver(10L, "sldj", "slkdjf"), car1);
        carService.getAll().forEach(System.out::println);
    }
}
