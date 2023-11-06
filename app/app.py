# Standard Flask
from flask import Flask, request, jsonify
from neo4j import GraphDatabase

app = Flask(__name__)

#####
##### Graph Database
#####

# Initialize the Neo4j driver - **Code from neo4j Developer Guides**
class Neo4jDatabase:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

# Define Neo4j connection parameters
neo4j_uri = "neo4j+s://ecc1e361.databases.neo4j.io"
neo4j_user = "neo4j"
neo4j_password = "ftkvgTQj1dtwCp-SNM8tf5OLuA7BV-ufo16TtZNMqFQ"

# Create a Neo4j database instance
neo4j_db = Neo4jDatabase(neo4j_uri, neo4j_user, neo4j_password)


#####
##### CRUD
#####


# CRUD Operations for Cars

def create_car_node(tx, make, model, year, location, status):
    # Create a car entry
    query = (
        "CREATE (car:Car {make: $make, model: $model, year: $year, location: $location, status: $status})"
        "RETURN id(car)"
    )
    result = tx.run(query, make=make, model=model, year=year, location=location, status=status)
    return result.single()[0]

def read_car_node(tx, car_id):
    # Get information about a car
    query = "MATCH (car:Car) WHERE id(car) = $car_id RETURN car"
    result = tx.run(query, car_id=car_id)
    return result.single()

def update_car_node(tx, car_id, make, model, year, location, status):
    # Update a car
    query = (
        "MATCH (car:Car) WHERE id(car) = $car_id "
        "SET car.make = $make, car.model = $model, car.year = $year, car.location = $location, car.status = $status"
    )
    tx.run(query, car_id=car_id, make=make, model=model, year=year, location=location, status=status)

def delete_car_node(tx, car_id):
    # Delete a car
    query = "MATCH (car:Car) WHERE id(car) = $car_id DETACH DELETE car"
    tx.run(query, car_id=car_id)


# CRUD Operations for Customers


def create_customer_node(tx, name, age, address):
    # Create a customer entry
    query = (
        "CREATE (customer:Customer {name: $name, age: $age, address: $address})"
        "RETURN id(customer)"
    )
    result = tx.run(query, name=name, age=age, address=address)
    return result.single()[0]

def read_customer_node(tx, customer_id):
    # Read a customer entry
    query = "MATCH (customer:Customer) WHERE id(customer) = $customer_id RETURN customer"
    result = tx.run(query, customer_id=customer_id)
    return result.single()

def update_customer_node(tx, customer_id, name, age, address):
    # Update a customer
    query = (
        "MATCH (customer:Customer) WHERE id(customer) = $customer_id "
        "SET customer.name = $name, customer.age = $age, customer.address = $address"
    )
    tx.run(query, customer_id=customer_id, name=name, age=age, address=address)

def delete_customer_node(tx, customer_id):
    # Delete a customer
    query = "MATCH (customer:Customer) WHERE id(customer) = $customer_id DETACH DELETE customer"
    tx.run(query, customer_id=customer_id)


# CRUD Operations for Employees


def create_employee_node(tx, name, address, branch):
    # Create an employee
    query = (
        "CREATE (employee:Employee {name: $name, address: $address, branch: $branch})"
        "RETURN id(employee)"
    )
    result = tx.run(query, name=name, address=address, branch=branch)
    return result.single()[0]

def read_employee_node(tx, employee_id):
    # Read an employee entry
    query = "MATCH (employee:Employee) WHERE id(employee) = $employee_id RETURN employee"
    result = tx.run(query, employee_id=employee_id)
    return result.single()

def update_employee_node(tx, employee_id, name, address, branch):
    # Update an employee
    query = (
        "MATCH (employee:Employee) WHERE id(employee) = $employee_id "
        "SET employee.name = $name, employee.address = $address, employee.branch = $branch"
    )
    tx.run(query, employee_id=employee_id, name=name, address=address, branch=branch)

def delete_employee_node(tx, employee_id):
    # Delete an employee
    query = "MATCH (employee:Employee) WHERE id(employee) = $employee_id DETACH DELETE employee"
    tx.run(query, employee_id=employee_id)


#####
##### CRUD endpoints
#####


# Create Car
@app.route('/create-car', methods=['POST'])
def create_car():
    data = request.get_json()
    make = data['make']
    model = data['model']
    year = data['year']
    location = data['location']
    status = data['status']

    with neo4j_db._driver.session() as session:
        car_id = session.write_transaction(create_car_node, make, model, year, location, status)
        return jsonify({'message': 'Car created successfully', 'car_id': car_id})

# Read Car
@app.route('/read-car/<int:car_id>', methods=['GET'])
def read_car(car_id):
    with neo4j_db._driver.session() as session:
        result = session.read_transaction(read_car_node, car_id)
        if result:
            return jsonify({'car': dict(result['car'])})
        else:
            return jsonify({'message': 'Car not found'}, 404)

# Update Car
@app.route('/update-car/<int:car_id>', methods=['PUT'])
def update_car(car_id):
    data = request.get_json()
    make = data['make']
    model = data['model']
    year = data['year']
    location = data['location']
    status = data['status']

    with neo4j_db._driver.session() as session:
        session.write_transaction(update_car_node, car_id, make, model, year, location, status)
        return jsonify({'message': 'Car updated successfully'})

# Delete Car
@app.route('/delete-car/<int:car_id>', methods=['DELETE'])
def delete_car(car_id):
    with neo4j_db._driver.session() as session:
        session.write_transaction(delete_car_node, car_id)
        return jsonify({'message': 'Car deleted successfully'})


#####
##### Car Booking Operations
#####


def order_car(tx, customer_id, car_id):
    # Check if the customer has not booked other cars
    existing_booking_query = (
        "MATCH (customer:Customer)-[:HAS_BOOKED]->(car:Car) "
        "WHERE id(customer) = $customer_id AND car.status = 'booked' "
        "RETURN car"
    )
    result = tx.run(existing_booking_query, customer_id=customer_id)
    existing_booking = result.single()
    
    if existing_booking:
        return None  # The customer has already booked a car

    # Change the status of the car to 'booked'
    booking_query = (
        "MATCH (customer:Customer), (car:Car) "
        "WHERE id(customer) = $customer_id AND id(car) = $car_id "
        "CREATE (customer)-[:HAS_BOOKED]->(car) "
        "SET car.status = 'booked' "
        "RETURN car"
    )
    result = tx.run(booking_query, customer_id=customer_id, car_id=car_id)
    return result.single()

def cancel_order_car(tx, customer_id, car_id):
    # Check if the customer has booked the car
    booking_query = (
        "MATCH (customer:Customer)-[:HAS_BOOKED]->(car:Car) "
        "WHERE id(customer) = $customer_id AND id(car) = $car_id "
        "RETURN car"
    )
    result = tx.run(booking_query, customer_id=customer_id, car_id=car_id)
    booking = result.single()
    
    if not booking:
        return None  # The customer has not booked this car

    # Change the status of the car to 'available'
    cancel_query = (
        "MATCH (customer:Customer)-[r:HAS_BOOKED]->(car:Car) "
        "WHERE id(customer) = $customer_id AND id(car) = $car_id "
        "DELETE r "
        "SET car.status = 'available' "
        "RETURN car"
    )
    result = tx.run(cancel_query, customer_id=customer_id, car_id=car_id)
    return result.single()

def rent_car(tx, customer_id, car_id):
    # Check if the customer has a booking for the car
    booking_query = (
        "MATCH (customer:Customer)-[:HAS_BOOKED]->(car:Car) "
        "WHERE id(customer) = $customer_id AND id(car) = $car_id "
        "RETURN car"
    )
    result = tx.run(booking_query, customer_id=customer_id, car_id=car_id)
    booking = result.single()
    
    if not booking:
        return None  # The customer has not booked this car

    # Change the status of the car to 'rented'
    rent_query = (
        "MATCH (customer:Customer)-[:HAS_BOOKED]->(car:Car) "
        "WHERE id(customer) = $customer_id AND id(car) = $car_id "
        "SET car.status = 'rented' "
        "RETURN car"
    )
    result = tx.run(rent_query, customer_id=customer_id, car_id=car_id)
    return result.single()

def return_car(tx, customer_id, car_id, status):
    # Check if the customer has rented the car
    rental_query = (
        "MATCH (customer:Customer)-[:HAS_BOOKED]->(car:Car) "
        "WHERE id(customer) = $customer_id AND id(car) = $car_id AND car.status = 'rented' "
        "RETURN car"
    )
    result = tx.run(rental_query, customer_id=customer_id, car_id=car_id)
    rental = result.single()
    
    if not rental:
        return None  # The customer has not rented this car

    # Change the status of the car to 'available' or 'damaged'
    return_query = (
        "MATCH (customer:Customer)-[r:HAS_BOOKED]->(car:Car) "
        "WHERE id(customer) = $customer_id AND id(car) = $car_id "
        "DELETE r "
        "SET car.status = $status "
        "RETURN car"
    )
    result = tx.run(return_query, customer_id=customer_id, car_id=car_id, status=status)
    return result.single()


#####
##### Car booking endpoints
#####


# Order Car
@app.route('/order-car', methods=['POST'])
def order_car_endpoint():
    data = request.get_json()
    customer_id = data['customer_id']
    car_id = data['car_id']

    with neo4j_db._driver.session() as session:
        result = session.write_transaction(order_car, customer_id, car_id)
        if result:
            return jsonify({'message': 'Car booked successfully', 'car_data': dict(result['car'])})
        else:
            return jsonify({'message': 'Car booking failed, the customer has already booked a car'}, 400)

# Cancel Order Car
@app.route('/cancel-order-car', methods=['POST'])
def cancel_order_car_endpoint():
    data = request.get_json()
    customer_id = data['customer_id']
    car_id = data['car_id']

    with neo4j_db._driver.session() as session:
        result = session.write_transaction(cancel_order_car, customer_id, car_id)
        if result:
            return jsonify({'message': 'Car booking canceled successfully', 'car_data': dict(result['car'])})
        else:
            return jsonify({'message': 'Car booking cancelation failed, the customer has not booked this car'}, 400)

# Rent Car
@app.route('/rent-car', methods=['POST'])
def rent_car_endpoint():
    data = request.get_json()
    customer_id = data['customer_id']
    car_id = data['car_id']

    with neo4j_db._driver.session() as session:
        result = session.write_transaction(rent_car, customer_id, car_id)
        if result:
            return jsonify({'message': 'Car rented successfully', 'car_data': dict(result['car'])})
        else:
            return jsonify({'message': 'Car rental failed, the customer has not booked this car'}, 400)

# Return Car
@app.route('/return-car', methods=['POST'])
def return_car_endpoint():
    data = request.get_json()
    customer_id = data['customer_id']
    car_id = data['car_id']
    status = data['status']  # 'available' or 'damaged'

    with neo4j_db._driver.session() as session:
        result = session.write_transaction(return_car, customer_id, car_id, status)
        if result:
            return jsonify({'message': 'Car returned successfully', 'car_data': dict(result['car'])})
        else:
            return jsonify({'message': 'Car return failed, the customer has not rented this car'}, 400)

# Standard Flask
if __name__ == '__main__':
    app.run(debug=True)
