package ceng.ceng351.foodrecdb;

import java.sql.*;
import java.util.ArrayList;

import ceng.ceng351.foodrecdb.IFOODRECDB;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class FOODRECDB implements IFOODRECDB {
    private Connection connection = null;

    @Override
    public void initialize() {
        String user = "e2449015"; // TODO: Your userName
        String password = "zDA827mOvqKG-EFH"; //  TODO: Your password
        String host = "momcorp.ceng.metu.edu.tr"; // host name
        String database = "db2449015"; // TODO: Your database name
        int port = 8080; // port

        String url = "jdbc:mysql://" + host + ":" + port + "/" + database;

        try {
            Class.forName("com.mysql.cj.jdbc.Driver");
            connection =  DriverManager.getConnection(url, user, password);
        }
        catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public int createTables() {
        int inserted = 0;

        /*
            • MenuItems(itemID:int, itemName:varchar(40), cuisine:varchar(20), price:int)
            • Ingredients(ingredientID:int, ingredientName:varchar(40))
            • Includes(itemID:int, ingredientID:int)
            • Ratings(ratingID:int, itemID:int, rating:int, ratingDate:date)
            • DietaryCategories(ingredientID:int, dietaryCategory:varchar(20))
        */

        String queryCreateMenuItems = "CREATE TABLE IF NOT EXISTS MenuItems (" +
                "itemID INT ," +
                "itemName VARCHAR(40) ," +
                "cuisine VARCHAR(20) ," +
                "price INT ," +
                "PRIMARY KEY (itemID))";

        String queryCreateIngredients = "CREATE TABLE IF NOT EXISTS Ingredients (" +
                "ingredientID INT ," +
                "ingredientName VARCHAR(40) ," +
                "PRIMARY KEY (ingredientID))";

        String queryCreateIncludes = "CREATE TABLE IF NOT EXISTS Includes (" +
                "itemID INT ," +
                "ingredientID INT ," +
                "PRIMARY KEY (itemID, ingredientID)," +
                "FOREIGN KEY (itemID) REFERENCES MenuItems(itemID) ON DELETE CASCADE ON UPDATE CASCADE," +
                "FOREIGN KEY (ingredientID) REFERENCES Ingredients(ingredientID) ON DELETE CASCADE ON UPDATE CASCADE)";

        String queryCreateRatings = "CREATE TABLE IF NOT EXISTS Ratings (" +
                "ratingID INT ," +
                "itemID INT ," +
                "rating INT ," +
                "ratingDate DATE ," +
                "PRIMARY KEY (ratingID), " +
                "FOREIGN KEY (itemID) REFERENCES MenuItems(itemID) ON DELETE CASCADE ON UPDATE CASCADE)";

        String queryCreateDietaryCategories = "CREATE TABLE IF NOT EXISTS DietaryCategories (" +
                "ingredientID INT ," +
                "dietaryCategory VARCHAR(20) ," +
                "PRIMARY KEY (ingredientID, dietaryCategory), " +
                "FOREIGN KEY (ingredientID) REFERENCES Ingredients(ingredientID) ON DELETE CASCADE ON UPDATE CASCADE)";

        try {
            Statement statement = connection.createStatement();
            statement.executeUpdate(queryCreateMenuItems);          inserted++;
            statement.executeUpdate(queryCreateIngredients);        inserted++;
            statement.executeUpdate(queryCreateIncludes);           inserted++;
            statement.executeUpdate(queryCreateRatings);            inserted++;
            statement.executeUpdate(queryCreateDietaryCategories);  inserted++;
            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return inserted;
    }

    @Override
    public int dropTables() {
        int dropped = 0;

        String queryDropMenuItems = "DROP TABLE IF EXISTS MenuItems";
        String queryDropIngredients = "DROP TABLE IF EXISTS Ingredients";
        String queryDropIncludes = "DROP TABLE IF EXISTS Includes";
        String queryDropRatings = "DROP TABLE IF EXISTS Ratings";
        String queryDropDietaryCategories = "DROP TABLE IF EXISTS DietaryCategories";

        try {
            Statement statement = this.connection.createStatement();
            statement.executeUpdate(queryDropDietaryCategories);  dropped++;
            statement.executeUpdate(queryDropRatings);            dropped++;
            statement.executeUpdate(queryDropIncludes);           dropped++;
            statement.executeUpdate(queryDropIngredients);        dropped++;
            statement.executeUpdate(queryDropMenuItems);          dropped++;
            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return dropped;
    }

    @Override
    public int insertMenuItems(MenuItem[] items) {
        int inserted = 0;

        for (MenuItem item : items) {
            String queryInsertMenuItem = "INSERT INTO MenuItems VALUES (" +
                    item.getItemID() + ", " +
                    "'" + item.getItemName() + "', " +
                    "'" + item.getCuisine() + "', " +
                    item.getPrice() + ")";

            try {
                Statement statement = this.connection.createStatement();
                statement.executeUpdate(queryInsertMenuItem);
                statement.close();
                inserted++;
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return inserted;
    }

    @Override
    public int insertIngredients(Ingredient[] ingredients) {
        int inserted = 0;

        for (Ingredient ingredient : ingredients) {
            String queryInsertIngredient = "INSERT INTO Ingredients VALUES (" +
                    ingredient.getIngredientID() + ", " +
                    "'" + ingredient.getIngredientName() + "')";

            try {
                Statement statement = this.connection.createStatement();
                statement.executeUpdate(queryInsertIngredient);
                statement.close();
                inserted++;
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return inserted;
    }

    @Override
    public int insertIncludes(Includes[] includes) {
        int inserted = 0;

        for (Includes include : includes) {
            String queryInsertIncludes = "INSERT INTO Includes VALUES (" +
                    include.getItemID() + ", " +
                    include.getIngredientID() + ")";

            try {
                Statement statement = this.connection.createStatement();
                statement.executeUpdate(queryInsertIncludes);
                statement.close();
                inserted++;
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return inserted;
    }

    @Override
    public int insertRatings(Rating[] ratings) {
        int inserted = 0;

        for (Rating rating : ratings) {
            String queryInsertRating = "INSERT INTO Ratings VALUES (" +
                    rating.getRatingID() + ", " +
                    rating.getItemID() + ", " +
                    rating.getRating() + ", " +
                    "'" + rating.getRatingDate() + "')";

            try {
                Statement statement = this.connection.createStatement();
                statement.executeUpdate(queryInsertRating);
                statement.close();
                inserted++;
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return inserted;
    }

    @Override
    public int insertDietaryCategories(DietaryCategory[] dietaryCategories) {
        int inserted = 0;

        for (DietaryCategory dietaryCategory : dietaryCategories) {
            String queryInsertDietaryCategory = "INSERT INTO DietaryCategories VALUES (" +
                    dietaryCategory.getIngredientID() + ", " +
                    "'" + dietaryCategory.getDietaryCategory() + "')";

            try {
                Statement statement = this.connection.createStatement();
                statement.executeUpdate(queryInsertDietaryCategory);
                statement.close();
                inserted++;
            }
            catch (SQLException e) {
                e.printStackTrace();
            }
        }

        return inserted;
    }

    @Override
    public MenuItem[] getMenuItemsWithGivenIngredient(String name) {
        String queryGetMenuItemsWithGivenIngredient = "SELECT * FROM MenuItems WHERE itemID IN (" +
                "SELECT itemID FROM Includes WHERE ingredientID IN (" +
                "SELECT ingredientID FROM Ingredients WHERE ingredientName = '" + name + "') )";

        ArrayList<MenuItem> menuItems = new ArrayList<>();

        try {
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery(queryGetMenuItemsWithGivenIngredient);

            while (resultSet.next()) {
                int itemID = resultSet.getInt("itemID");
                String itemName = resultSet.getString("itemName");
                String cuisine = resultSet.getString("cuisine");
                int price = resultSet.getInt("price");

                menuItems.add(new MenuItem(itemID, itemName, cuisine, price));
            }

            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return menuItems.toArray(new MenuItem[menuItems.size()]);
    }

    @Override
    public MenuItem[] getMenuItemsWithoutAnyIngredient() {
        String queryGetMenuItemsWithoutAnyIngredient = "SELECT * FROM MenuItems WHERE itemID NOT IN (" +
                "SELECT itemID FROM Includes) ";

        ArrayList<MenuItem> menuItems = new ArrayList<>();

        try {
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery(queryGetMenuItemsWithoutAnyIngredient);

            while (resultSet.next()) {
                int itemID = resultSet.getInt("itemID");
                String itemName = resultSet.getString("itemName");
                String cuisine = resultSet.getString("cuisine");
                int price = resultSet.getInt("price");

                menuItems.add(new MenuItem(itemID, itemName, cuisine, price));
            }

            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return menuItems.toArray(new MenuItem[menuItems.size()]);
    }

    @Override
    public Ingredient[] getNotIncludedIngredients(){
        String queryGetNotIncludedIngredients = "SELECT * FROM Ingredients WHERE ingredientID NOT IN (" +
                "SELECT ingredientID FROM Includes) ";

        ArrayList<Ingredient> ingredients = new ArrayList<>();

        try {
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery(queryGetNotIncludedIngredients);

            while (resultSet.next()) {
                int ingredientID = resultSet.getInt("ingredientID");
                String ingredientName = resultSet.getString("ingredientName");

                ingredients.add(new Ingredient(ingredientID, ingredientName));
            }

            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return ingredients.toArray(new Ingredient[ingredients.size()]);
    }

    @Override
    public MenuItem getMenuItemWithMostIngredients(){
        String queryGetMenuItemWithMostIngredients = "SELECT * FROM MenuItems WHERE itemID IN (" +
                "SELECT itemID FROM Includes GROUP BY itemID HAVING COUNT(itemID) = (" +
                "SELECT MAX(count) FROM (" +
                "SELECT itemID, COUNT(itemID) AS count FROM Includes GROUP BY itemID) as SUB1 ) )";

        MenuItem menuItem = null;

        try {
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery(queryGetMenuItemWithMostIngredients);

            while (resultSet.next()) {
                int itemID = resultSet.getInt("itemID");
                String itemName = resultSet.getString("itemName");
                String cuisine = resultSet.getString("cuisine");
                int price = resultSet.getInt("price");

                menuItem = new MenuItem(itemID, itemName, cuisine, price);
            }

            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return menuItem;
    }

    @Override
    public QueryResult.MenuItemAverageRatingResult[] getMenuItemsWithAvgRatings(){
        String queryGetMenuItemsWithAvgRatings = "SELECT itemID, itemName, cuisine, price, AVG(rating) AS avgRating FROM MenuItems NATURAL JOIN Ratings GROUP BY itemID";

        ArrayList<QueryResult.MenuItemAverageRatingResult> menuItemAverageRatingResults = new ArrayList<>();

        try {
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery(queryGetMenuItemsWithAvgRatings);

            while (resultSet.next()) {
                String itemID = resultSet.getString("itemID");
                String itemName = resultSet.getString("itemName");
                String cuisine = resultSet.getString("cuisine");
                int price = resultSet.getInt("price");
                String avgRating = resultSet.getString("avgRating");

                menuItemAverageRatingResults.add(new QueryResult.MenuItemAverageRatingResult(itemID, itemName, avgRating));
            }

            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return menuItemAverageRatingResults.toArray(new QueryResult.MenuItemAverageRatingResult[menuItemAverageRatingResults.size()]);
    }

    @Override
    public MenuItem[] getMenuItemsForDietaryCategory(String category){
        String queryGetMenuItemsForDietaryCategory = "SELECT * FROM MenuItems WHERE itemID IN (" +
                "SELECT itemID FROM Includes WHERE ingredientID IN (" +
                "SELECT ingredientID FROM DietaryCategories WHERE dietaryCategory = '" + category + "') )";

        ArrayList<MenuItem> menuItems = new ArrayList<>();

        try {
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery(queryGetMenuItemsForDietaryCategory);

            while (resultSet.next()) {
                int itemID = resultSet.getInt("itemID");
                String itemName = resultSet.getString("itemName");
                String cuisine = resultSet.getString("cuisine");
                int price = resultSet.getInt("price");

                menuItems.add(new MenuItem(itemID, itemName, cuisine, price));
            }

            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return menuItems.toArray(new MenuItem[menuItems.size()]);
    }

    @Override
    public Ingredient getMostUsedIngredient(){
        String queryGetMostUsedIngredient = "SELECT * FROM Ingredients WHERE ingredientID IN (" +
                "SELECT ingredientID FROM Includes GROUP BY ingredientID HAVING COUNT(ingredientID) = (" +
                "SELECT MAX(count) FROM (" +
                "SELECT ingredientID, COUNT(ingredientID) AS count FROM Includes GROUP BY ingredientID) as SUB1 ) )";

        Ingredient ingredient = null;

        try {
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery(queryGetMostUsedIngredient);

            resultSet.next();
            int ingredientID = resultSet.getInt("ingredientID");
            String ingredientName = resultSet.getString("ingredientName");

            ingredient = new Ingredient(ingredientID, ingredientName);

            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return ingredient;
    }

    @Override
    public QueryResult.CuisineWithAverageResult[] getCuisinesWithAvgRating(){
        String queryGetCuisinesWithAvgRating = "SELECT cuisine, AVG(rating) AS avgRating FROM MenuItems NATURAL JOIN Ratings GROUP BY cuisine";

        ArrayList<QueryResult.CuisineWithAverageResult> cuisineWithAverageResults = new ArrayList<>();

        try {
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery(queryGetCuisinesWithAvgRating);

            while (resultSet.next()) {
                String cuisine = resultSet.getString("cuisine");
                String avgRating = resultSet.getString("avgRating");

                cuisineWithAverageResults.add(new QueryResult.CuisineWithAverageResult(cuisine, avgRating));
            }

            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return cuisineWithAverageResults.toArray(new QueryResult.CuisineWithAverageResult[cuisineWithAverageResults.size()]);
    }

    @Override
    public QueryResult.CuisineWithAverageResult[] getCuisinesWithAvgIngredientCount(){
        String queryGetCuisinesWithAvgIngredientCount = "SELECT cuisine, AVG(count) AS avgCount FROM (" +
                "SELECT cuisine, COUNT(ingredientID) AS count FROM MenuItems NATURAL JOIN Includes GROUP BY itemID) as SUB  GROUP BY cuisine";

        ArrayList<QueryResult.CuisineWithAverageResult> cuisineWithAverageResults = new ArrayList<>();

        try {
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery(queryGetCuisinesWithAvgIngredientCount);

            while (resultSet.next()) {
                String cuisine = resultSet.getString("cuisine");
                String avgCount = resultSet.getString("avgCount");

                cuisineWithAverageResults.add(new QueryResult.CuisineWithAverageResult(cuisine, avgCount));
            }

            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return cuisineWithAverageResults.toArray(new QueryResult.CuisineWithAverageResult[cuisineWithAverageResults.size()]);
    }

    @Override
    public int increasePrice(String ingredientName, String increaseAmount){
        String queryIncreasePrice = "UPDATE MenuItems SET price = price + " + increaseAmount + " WHERE itemID IN (" +
                "SELECT itemID FROM Includes WHERE ingredientID IN (" +
                "SELECT ingredientID FROM Ingredients WHERE ingredientName = '" + ingredientName + "') )";

        int rowsAffected = 0;

        try {
            Statement statement = this.connection.createStatement();
            rowsAffected = statement.executeUpdate(queryIncreasePrice);

            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return rowsAffected;
    }

    @Override
    public Rating[] deleteOlderRatings(String date){
        String querySelectOlderRatings = "SELECT * FROM Ratings WHERE ratingDate < '" + date + "'";
        String queryDeleteOlderRatings = "DELETE FROM Ratings WHERE ratingDate < '" + date + "'";

        ArrayList<Rating> ratings = new ArrayList<>();

        try {
            Statement statement = this.connection.createStatement();
            ResultSet resultSet = statement.executeQuery(querySelectOlderRatings);

            while (resultSet.next()) {
                int ratingID = resultSet.getInt("ratingID");
                int itemID = resultSet.getInt("itemID");
                int rating = resultSet.getInt("rating");
                String resultDate = resultSet.getString("ratingDate");

                ratings.add(new Rating(ratingID, itemID, rating, resultDate));
            }

            statement.executeUpdate(queryDeleteOlderRatings);

            statement.close();
        }
        catch (SQLException e) {
            e.printStackTrace();
        }

        return ratings.toArray(new Rating[ratings.size()]);
    }
}
