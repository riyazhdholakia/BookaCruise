var db = firebase.firestore();
function addNames() {
    var firstName = document.getElementById("firstName_field").value;
    var lastName = document.getElementById("lastName_field").value;
    var email = document.getElementById("email_field").value;
    var destination = "";
    var suite = "";
    var date = "";
    var confirmationNum= "";
    // Add a new document in collection "cities"
    db.collection("users").doc().set({
        firstName: firstName,
        lastName: lastName,
        email: email,
        destination: destination,
        suite: suite,
        date: date,
        confirmationNum: confirmationNum
    })
        .then(function () {
            console.log("Document successfully written!");
        })
        .catch(function (error) {
            console.error("Error writing document: ", error);
        });
}

var user = firebase.auth().currentUser;

function addDestination() {
    var destination = document.getElementById("destination").innerHTML;
    // Add a new document in collection "cities"
    db.collection(user.currentUser).doc(user.currentUser.document).update({
        destination: destination
    })
        .then(function () {
            console.log("Document successfully written!");
            window.open('room.html', '_self');
        })
        .catch(function (error) {
            console.error("Error writing document: ", error);
        });
}
