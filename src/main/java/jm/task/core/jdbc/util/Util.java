package jm.task.core.jdbc.util;

import jm.task.core.jdbc.model.User;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.cfg.Environment;
import org.hibernate.service.ServiceRegistry;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

public class Util {
    // реализуйте настройку соеденения с БД
    private static final String DB_DRIVER = "com.mysql.cj.jdbc.Driver";
    private static final String DB_URL = "jdbc:mysql://localhost:3306/db";
    private static final String DB_USER = "root";
    private static final String DB_PASSWORD = "2443254";
    private static final String DIALECT = "org.hibernate.dialect.MySQL5Dialect";
    private static final String SHOW_SQL = "true";
    private static final String CURRENT_SESSION_CONTEXT_CLASS = "thread";
    private static final String HBM2DDL_AUTO = "update";


    private static SessionFactory sessionFactory;
    private static Logger logger = Logger.getLogger(Util.class.getName());


    private Util() {

    }

    public static Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName(DB_DRIVER);
            connection = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            connection.setAutoCommit(false);
            logger.info("Соединение с БД установлено");
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();

        }
        return connection;
    }


    public static void closeConnection(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
                logger.info("Соединение с БД закрыто");
            } catch (SQLException e) {
                e.printStackTrace();
                logger.info("Ошибка при закрытии соединения с БД");
            }
        } else {
            logger.info("Соединение уже было закрыто или не инициализировано");
        }
    }

    public static SessionFactory getSessionFactory() {
        if (sessionFactory == null) {
            try {
                Configuration configuration = new Configuration();

                // Hibernate settings equivalent to hibernate.cfg.xml's properties
                Properties settings = new Properties();
                settings.put(Environment.DRIVER, DB_DRIVER);
                settings.put(Environment.URL, DB_URL);
                settings.put(Environment.USER, DB_USER);
                settings.put(Environment.PASS, DB_PASSWORD);
                settings.put(Environment.DIALECT, DIALECT);

                settings.put(Environment.SHOW_SQL, SHOW_SQL);

                settings.put(Environment.CURRENT_SESSION_CONTEXT_CLASS, CURRENT_SESSION_CONTEXT_CLASS);

                settings.put(Environment.HBM2DDL_AUTO, HBM2DDL_AUTO);

                configuration.setProperties(settings);

                configuration.addAnnotatedClass(User.class);

                ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder()
                        .applySettings(configuration.getProperties())
                        .build();

                sessionFactory = configuration.buildSessionFactory(serviceRegistry);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return sessionFactory;
    }


    public static void closeSessionFactory() {
        if (sessionFactory != null && sessionFactory.isOpen()) {
            sessionFactory.close();
        }
    }
}