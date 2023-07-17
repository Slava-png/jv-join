package mate.jdbc.dao.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import mate.jdbc.dao.CarDao;
import mate.jdbc.exception.DataProcessingException;
import mate.jdbc.lib.Dao;
import mate.jdbc.model.Car;
import mate.jdbc.model.Driver;
import mate.jdbc.model.Manufacturer;
import mate.jdbc.util.ConnectionUtil;

@Dao
public class CarDaoImpl implements CarDao {
    @Override
    public Car create(Car car) {
        String insertStatement = "INSERT INTO cars (model, manufacturer_id) VALUES (?, ?);";

        try (Connection connection = ConnectionUtil.getConnection();
                PreparedStatement createStatement = connection
                        .prepareStatement(insertStatement, Statement.RETURN_GENERATED_KEYS);) {

            createStatement.setString(1, car.getModel());
            createStatement.setLong(2, car.getManufacturer().getId());
            createStatement.executeUpdate();

            ResultSet generatedKeys = createStatement.getGeneratedKeys();
            if (generatedKeys.next()) {
                car.setId(generatedKeys.getObject(1, Long.class));
            }
        } catch (SQLException e) {
            throw new DataProcessingException("Can't create car " + car, e);
        }
        createDriverForCarInManyToMany(car);
        return car;
    }

    private void createDriverForCarInManyToMany(Car car) {
        String query = "INSERT INTO cars_drivers (car_id, driver_id) VALUES (?, ?);";

        try (Connection connection = ConnectionUtil.getConnection();
                 PreparedStatement insertStatement = connection
                         .prepareStatement(query, Statement.RETURN_GENERATED_KEYS);) {
            insertStatement.setLong(1, car.getId());

            for (Driver driver: car.getDrivers()) {
                insertStatement.setLong(2, driver.getId());
                insertStatement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new DataProcessingException("Can't insert driver to car " + car, e);
        }
    }

    @Override
    public Optional<Car> get(Long id) {
        String query = "SELECT c.id,  c.model, m.id AS manufacurer_id, m.name, "
                + "country, d.id AS driver_id, d.name, license_number "
                + "FROM cars AS c "
                + "JOIN manufacturers AS m ON c.manufacturer_id = m.id "
                + "JOIN cars_drivers AS cd ON cd.car_id = c.id "
                + "JOIN drivers AS d ON d.id = cd.driver_id "
                + "WHERE c.id = ? AND c.is_deleted=FALSE "
                + "AND m.is_deleted=FALSE AND d.is_deleted=FALSE;";

        try (Connection connection = ConnectionUtil.getConnection();
                 PreparedStatement getStatement = connection.prepareStatement(query);) {
            getStatement.setLong(1, id);

            ResultSet resultSet = getStatement.executeQuery();
            Car car = null;
            if (resultSet.next()) {
                car = parseResultSetForCar(resultSet);
                do {
                    car.addDriver(parseResultSetForDrivers(resultSet));
                } while (resultSet.next());
            }
            return Optional.ofNullable(car);
        } catch (SQLException e) {
            throw new DataProcessingException("Can't get car with id " + id, e);
        }
    }

    @Override
    public List<Car> getAll() {
        String query = "SELECT c.id,  c.model, m.id AS manufacurer_id, m.name, "
                + "country, d.id AS driver_id, d.name, license_number "
                + "FROM cars AS c "
                + "JOIN manufacturers AS m ON c.manufacturer_id = m.id "
                + "JOIN cars_drivers AS cd ON cd.car_id = c.id "
                + "JOIN drivers AS d ON d.id = cd.driver_id "
                + "WHERE c.is_deleted=FALSE AND m.is_deleted=FALSE "
                + "AND d.is_deleted=FALSE;";

        try (Connection connection = ConnectionUtil.getConnection();
                 PreparedStatement getAllStatement = connection.prepareStatement(query);) {
            ResultSet resultSet = getAllStatement.executeQuery();

            Map<Long, Car> carsMap = new HashMap<>();
            while (resultSet.next()) {
                Long carId = resultSet.getObject("c.id", Long.class);

                if (! carsMap.containsKey(carId)) {
                    carsMap.put(carId, parseResultSetForCar(resultSet));
                }
                Driver driver = parseResultSetForDrivers(resultSet);
                carsMap.get(carId).addDriver(driver);
            }
            return new ArrayList<>(carsMap.values());
        } catch (SQLException e) {
            throw new DataProcessingException("Can't get all cars", e);
        }
    }

    @Override
    public Car update(Car car) {
        String query = "UPDATE cars SET model=?, manufacturer_id=? "
                + "WHERE id=? AND is_deleted=FALSE;";

        try (Connection connection = ConnectionUtil.getConnection();
                 PreparedStatement updateStatement = connection.prepareStatement(query)) {
            updateStatement.setString(1, car.getModel());
            updateStatement.setLong(2, car.getManufacturer().getId());
            updateStatement.setLong(3, car.getId());
            updateStatement.executeUpdate();

        } catch (SQLException e) {
            throw new DataProcessingException("Couldn't update car "
                    + car + " in carDB", e);
        }

        deleteAllDriversFromCar(car.getId());
        addNewDriversToCar(car);
        return car;
    }

    private void deleteAllDriversFromCar(long carId) {
        String query = "DELETE FROM cars_drivers WHERE car_id=?;";

        try (Connection connection = ConnectionUtil.getConnection();
                 PreparedStatement deleteStatement = connection.prepareStatement(query);) {
            deleteStatement.setLong(1, carId);
            deleteStatement.executeUpdate();
        } catch (SQLException e) {
            throw new DataProcessingException("Couldn't remove drivers from car with id "
                    + carId + " in carDB", e);
        }
    }

    private void addNewDriversToCar(Car car) {
        String query = "INSERT INTO cars_drivers (car_id, driver_id) VALUES (?, ?);";

        try (Connection connection = ConnectionUtil.getConnection();
                 PreparedStatement insertStatement = connection.prepareStatement(query);) {
            insertStatement.setLong(1, car.getId());
            for (Driver driver: car.getDrivers()) {
                insertStatement.setLong(2, driver.getId());
                insertStatement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new DataProcessingException("Couldn't add drivers to car "
                    + car + " in carDB", e);
        }
    }

    @Override
    public boolean delete(Long id) {
        String query = "UPDATE cars SET is_deleted=TRUE WHERE id = ?;";

        try (Connection connection = ConnectionUtil.getConnection();
                 PreparedStatement updateStatement = connection.prepareStatement(query);) {
            updateStatement.setLong(1, id);
            return updateStatement.executeUpdate() != 0;
        } catch (SQLException e) {
            throw new DataProcessingException("Can't delete car with id " + id, e);
        }
    }

    @Override
    public List<Car> getAllByDriver(Long driverId) {
        String query = "SELECT c.id, c.model, m.id AS manufacurer_id, m.name, country "
                + "FROM drivers AS d "
                + "JOIN cars_drivers AS cd ON d.id = cd.driver_id "
                + "JOIN cars AS c ON cd.car_id = c.id "
                + "JOIN manufacturers AS m ON m.id = c.manufacturer_id "
                + "WHERE d.id = ? AND c.is_deleted=FALSE "
                + "AND m.is_deleted=FALSE AND d.is_deleted=FALSE;";

        try (Connection connection = ConnectionUtil.getConnection();
                 PreparedStatement getByDriverStatement = connection.prepareStatement(query);) {
            getByDriverStatement.setLong(1, driverId);
            ResultSet resultSet = getByDriverStatement.executeQuery();

            List<Car> cars = new ArrayList<>();
            while (resultSet.next()) {
                cars.add(parseResultSetForCar(resultSet));
            }
            return cars;
        } catch (SQLException e) {
            throw new DataProcessingException("Couldn't get all cars by driver with id "
                    + driverId, e);
        }
    }

    private Car parseResultSetForCar(ResultSet resultSet) throws SQLException {
        Manufacturer manufacturer = new Manufacturer();
        manufacturer.setId(resultSet.getObject("manufacurer_id", Long.class));
        manufacturer.setCountry(resultSet.getString("country"));
        manufacturer.setName(resultSet.getString("m.name"));

        Long carId = resultSet.getObject("c.id", Long.class);
        String carModel = resultSet.getString("c.model");
        return new Car(carId, carModel, manufacturer);
    }

    private Driver parseResultSetForDrivers(ResultSet resultSet) throws SQLException {
        Long id = resultSet.getObject("driver_id", Long.class);
        String name = resultSet.getString("d.name");
        String licenseNumber = resultSet.getString("license_number");

        return new Driver(id, name, licenseNumber);
    }
}
