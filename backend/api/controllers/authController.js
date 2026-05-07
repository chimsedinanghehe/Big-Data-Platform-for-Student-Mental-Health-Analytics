const {
    findUserByEmail,
    createUser
} = require("../../models/userModel");

const {
    hashPassword,
    comparePassword
} = require("../../services/authService");

const generateToken = require("../../utils/jwt");

exports.register = async (req, res) => {
    try {
        const { username, email, password } = req.body;

        if (!username || !email || !password) {
            return res.status(400).json({
                message: "Missing fields"
            });
        }

        findUserByEmail(email, async (err, result) => {
            if (err) {
                return res.status(500).json(err);
            }

            if (result.length > 0) {
                return res.status(400).json({
                    message: "Email already exists"
                });
            }

            const hashedPassword = await hashPassword(password);

            createUser(
                username,
                email,
                hashedPassword,
                (err, result) => {

                    if (err) {
                        return res.status(500).json(err);
                    }

                    res.status(201).json({
                        message: "Register successful"
                    });
                }
            );
        });

    } catch (error) {
        res.status(500).json(error);
    }
};

exports.login = (req, res) => {
    try {
        const { email, password } = req.body;

        findUserByEmail(email, async (err, result) => {

            if (err) {
                return res.status(500).json(err);
            }

            if (result.length === 0) {
                return res.status(400).json({
                    message: "User not found"
                });
            }

            const user = result[0];

            const isMatch = await comparePassword(
                password,
                user.password
            );

            if (!isMatch) {
                return res.status(400).json({
                    message: "Wrong password"
                });
            }

            const token = generateToken(user);

            res.json({
                message: "Login successful",

                token,

                user: {
                    id: user.id,
                    username: user.username,
                    email: user.email
                }
            });
        });

    } catch (error) {
        res.status(500).json(error);
    }
};