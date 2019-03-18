var db = firebase.firestore();
function addNames() {
    var firstName = document.getElementById("firstName_field").value;
    var lastName = document.getElementById("lastName_field").value;
    var email = document.getElementById("email_field").value;
    // Add a new document in collection "cities"
    db.collection("users").doc().set({
        firstName: firstName,
        lastName: lastName,
        email: email  
    })
        .then(function () {
            console.log("Document successfully written!");
        })
        .catch(function (error) {
            console.error("Error writing document: ", error);
        });
}