from flask import Flask
from flask_restful import Api, Resource, reqparse
from flask_sqlalchemy import SQLAlchemy
import uuid
import asyncio
import random
import json
import argparse
import datetime
from threading import Thread

app = Flask(__name__)
api = Api(app)

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:////tmp/pypay_database.db'
app.config['SQLALCHEMY_BINDS'] = {
    'message_queue': 'sqlite:///../var/data.db'
}
db = SQLAlchemy()
db.init_app(app)

class Payment(db.Model):
    uuid = db.Column(db.String(36), primary_key=True)
    invoice_id = db.Column(db.Integer)
    status = db.Column(db.String(20))

# make --chaotic optional
parser = argparse.ArgumentParser()
parser.add_argument('--chaotic', action='store_true')
args = parser.parse_known_args()
is_chaotic = args[0].chaotic

def randomly_fail():
    global is_chaotic
    if is_chaotic & (random.randint(0, 10) == 0):
        return True
    return False

class MessengerMessage(db.Model):
    __tablename__ = 'messenger_messages'
    __bind_key__ = 'message_queue'

    id = db.Column(db.Integer, primary_key=True)
    body = db.Column(db.Text)
    headers = db.Column(db.Text)
    queue_name = db.Column(db.String(190))
    created_at = db.Column(db.DateTime)
    available_at = db.Column(db.DateTime)
    delivered_at = db.Column(db.DateTime)

class PaymentAPI(Resource):
    def put(self, payment_id):
        if randomly_fail():
            return {'message': 'Service not available'}, 500

        payment = Payment.query.filter_by(uuid=payment_id).first()
        if not payment:
            return {'message': 'Payment not found'}, 404

        if payment.status != 'pending':
            return {'message': 'Payment already processed'}, 400

        # Payment can only be set to "paid" or "failed"
        parser = reqparse.RequestParser()
        parser.add_argument('status', type=str, required=True)
        args = parser.parse_args()

        if args['status'] not in ['paid', 'failed']:
            return {'message': 'Invalid status'}, 400

        payment.status = args['status']
        db.session.commit()

        return {'message': 'Payment updated'}, 200


    def delete(self, payment_id):
        if randomly_fail():
            return {'message': 'Service not available'}, 500
        payment = Payment.query.filter_by(uuid=payment_id).first()
        if not payment:
            return {'message': 'Payment not found'}, 404

        if payment.status != 'pending':
            return {'message': 'Payment already processed'}, 400

        payment.status = 'aborted'
        db.session.commit()

        return {'message': 'Payment aborted'}, 200

class PaymentList(Resource):
    def post(self):
        if randomly_fail():
            return {'message': 'Service not available'}, 500
        parser = reqparse.RequestParser()
        parser.add_argument('invoice_id', type=int, required=True)
        args = parser.parse_args()

        payment = Payment(uuid=str(uuid.uuid4()), invoice_id=args['invoice_id'], status='pending')
        db.session.add(payment)
        db.session.commit()

        thread = Thread(target=asyncio.run, args=(send_random_payment_confirmation(payment.invoice_id),))
        thread.start()

        return {'message': 'Payment requested', 'payment_id': payment.uuid}, 201

async def send_random_payment_confirmation(invoice_id):
    await asyncio.sleep(3)
    print('Payment confirmation for payment %s' % invoice_id)
    message_type = '\App\Message\PaymentFailed' if random.randint(0, 3) > 0 else '\App\Message\PaymentConfirmed'

    message = MessengerMessage(
        body=json.dumps({'invoiceId': invoice_id}),
        headers=json.dumps({'Content-Type': 'application/json', 'type': message_type}),
        queue_name='payment_confirmation',
        created_at=datetime.datetime.now(),
        available_at=datetime.datetime.now()
    )
    with app.app_context():
        db.session.add(message)
        db.session.commit()

api.add_resource(PaymentList, '/payment')
api.add_resource(PaymentAPI, '/payment/<string:payment_id>')

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    loop = asyncio.get_event_loop()
    loop.run_until_complete(asyncio.gather(
        loop.create_task(app.run(debug=True))
    ))
