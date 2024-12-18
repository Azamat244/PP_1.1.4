package jm.task.core.jdbc.dao;

import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import static jm.task.core.jdbc.util.Util.closeConnection;

public class UserDaoJDBCImpl implements UserDao {
    private Connection connection = Util.getConnection();
    private static final Logger logger = Logger.getLogger(UserDaoJDBCImpl.class.getName());


    public UserDaoJDBCImpl() {

    }

    @Override
    public void createUsersTable() {
        String sql = "CREATE TABLE IF NOT EXISTS users (" +
                "id INT AUTO_INCREMENT PRIMARY KEY," +
                "name VARCHAR(100), " +
                "last_name VARCHAR(100), " +
                "age INT" +
                ")";

        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            logger.info("Таблица создана");
        } catch (SQLException e) {
            throw new RuntimeException("Ошибка создания таблицы", e);
        } finally {
            closeConnection(connection);
        }


    }

    @Override
    public void dropUsersTable() {
        String sql = "DROP TABLE IF EXISTS users";
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            logger.info("Таблица удалена");
        } catch (SQLException e) {
            throw new RuntimeException("Ошибка удаления таблицы", e);
        } finally {
            closeConnection(connection);
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {
        String sql = "INSERT INTO users (name, last_name, age) VALUES (?, ?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setInt(3, age);
            preparedStatement.executeUpdate();
            logger.info("User с именем — " + name + " добавлен в базу данных");
        } catch (SQLException e) {
            try {
                connection.commit();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }

            throw new RuntimeException("Ошибка добавления пользователя", e);
        } finally {
            closeConnection(connection);
        }
    }

    @Override
    public void removeUserById(long id) {
        String sql = "DELETE FROM users WHERE id = ?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
            logger.info("Пользователь с id = " + id + " удален");
            connection.commit();

        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            throw new RuntimeException("Ошибка удаления пользователя по id", e);
        } finally {
            closeConnection(connection);
        }

    }

    @Override
    public List<User> getAllUsers() {
        String sql = "SELECT * FROM users";
        List<User> users = new ArrayList<>();
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                User user = new User();
                user.setId(resultSet.getLong("id"));
                user.setName(resultSet.getString("name"));
                user.setLastName(resultSet.getString("last_name"));
                user.setAge(resultSet.getByte("age"));
                users.add(user);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Ошибка получения данных из таблицы", e);
        } finally {
            closeConnection(connection);
        }
        System.out.println(users);
        return users;
    }

    @Override
    public void cleanUsersTable() {
        String sql = "DELETE FROM users";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.executeUpdate();

            connection.commit();
        } catch (SQLException e) {
            try {
                connection.rollback();
            } catch (SQLException ex) {
                throw new RuntimeException(ex);
            }
            throw new RuntimeException("Ошибка удаления данных из таблицы", e);
        } finally {
            closeConnection(connection);
        }

    }
}
