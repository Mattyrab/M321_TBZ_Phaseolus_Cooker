namespace CookerBeaners.Models;

public record OrderItem(string Name, int Quantity);

// payment.succeeded payload, as published by PayBeancoins
public record PaymentSucceededPayload(decimal AmountCharged, List<OrderItem> Items);

// order.cooking-started payload — orderId (on the envelope) alone is sufficient, per Q5
public record OrderCookingStartedPayload();

// order.cooked payload — orderId (on the envelope) alone is sufficient, per Q5
public record OrderCookedPayload();

// Per-item cook time in seconds. Items cook in parallel (separate burners), so an
// order's total cook time is its slowest item, not the sum of all of them.
public static class CookTimes
{
    private const int DefaultSeconds = 30;

    private static readonly Dictionary<string, int> BySeconds = new(StringComparer.OrdinalIgnoreCase)
    {
        ["Classic Beans"] = 15,
        ["Spicy Beans"] = 25,
        ["Bean Chili"] = 50,
        ["Bean Burrito"] = 80,
    };

    public static int ForOrder(IReadOnlyList<OrderItem> items) =>
        items.Count == 0 ? DefaultSeconds : items.Max(i => BySeconds.GetValueOrDefault(i.Name, DefaultSeconds));
}
