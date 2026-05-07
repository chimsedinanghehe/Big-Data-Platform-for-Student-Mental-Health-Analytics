const db = require("../database/db");

const findUserByEmail = (email, callback) => {
    const sql = "SELECT * FROM users WHERE email = ?";

    db.query(sql, [email], callback);
};

const createUser = (username, email, password, callback) => {
    const sql = `
        INSERT INTO users(username, email, password)
        VALUES (?, ?, ?)
    `;

    db.query(sql, [username, email, password], callback);
};

module.exports = {
    findUserByEmail,
    createUser
};